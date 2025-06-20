#!/usr/bin/env perl
#
# This test covers pgactive_init_copy support for using a pre-existing database
# running as a streaming replica before being promoted to pgactive.
#
# Sync-up can be by any method including WAL archiving, streaming, etc.
#
use strict;
use warnings;
use lib 'test/t/';
use Cwd;
use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use utils::nodemanagement;

my $tempdir = PostgreSQL::Test::Utils::tempdir;

my $node_a = PostgreSQL::Test::Cluster->new('node-a');
initandstart_node($node_a, $pgactive_test_dbname, extra_init_opts => { has_archiving => 1 });
create_pgactive_group($node_a);

my $backup_name = 'a_back1';
$node_a->backup_fs_cold($backup_name);

my $node_b = PostgreSQL::Test::Cluster->new('node-b');
$node_b->init_from_backup(
	$node_a, $backup_name,
	has_streaming => 1,
	has_restoring => 1);

# We don't have to remove pgactive from shared_preload_libraries; pg won't start
# workers until recovery ends, and pgactive_init_copy will disable pgactive when it
# promotes the server and clears out the catalogs.
#
# The postgresql.conf copied over will use the same port as node_a, so fix that.
#
$node_b->append_conf('postgresql.conf', "port = " . $node_b->port . "\n");
$node_b->start;

$node_b->safe_psql($pgactive_test_dbname, 'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots;');
exec_ddl($node_a, q[CREATE TABLE public.initialdata (a integer);]);
$node_a->safe_psql($pgactive_test_dbname, q[INSERT INTO initialdata(a) VALUES (1);]);

$node_a->wait_for_catchup($node_b, 'replay', $node_a->lsn('flush'));

$node_b->stop;

$node_a->safe_psql($pgactive_test_dbname, q[INSERT INTO initialdata(a) VALUES (2);]);

# We don't have to wait for the node to catch up, as pgactive_init_copy will ensure
# that happens for us, promoting it only once it's passed the replay position
# where pgactive_init_copy created a slot on the upstream.
command_ok(
    [
        'pgactive_init_copy', '-v',
        '-D', $node_b->data_dir,
        "-n", 'node-b',
        '-d', $node_a->connstr($pgactive_test_dbname),
        '--local-dbname', $pgactive_test_dbname,
        '--local-port', $node_b->port,
        '--log-file', $node_b->logfile . "_initcopy"
    ],
	'pgactive_init_copy succeeds');

# PostgresNode doesn't know the DB got restarted
$node_b->_update_pid(1);

# Make sure recovery stopped at pgactive_init_copy's recovery point not our faked up one
my $found = 0;
my $line;
my $fh;
open ($fh, $node_b->logfile . "_initcopy")
	or die ("failed to open init copy log file: $!");
while ($line = <$fh>)
{
	if ($line =~ 'recovery stopping at restore point "')
	{
		$found = 1;
		last;
	}
}
close($fh);
like($line, qr/"pgactive_/, "recovery stopped at pgactive restore point");

is($node_a->safe_psql($pgactive_test_dbname, 'SELECT a FROM initialdata ORDER BY a'),
	"1\n2",
	'Initial data copied');

# OK, pgactive should be up.
#
# TODO reuse everything after here instead of copying it from t/010
#
my $pgactive_version = $node_b->safe_psql($pgactive_test_dbname, 'SELECT pgactive.pgactive_version()');
note "pgactive version $pgactive_version";

$node_a->safe_psql($pgactive_test_dbname,
	qq[SELECT pgactive.pgactive_wait_for_node_ready($PostgreSQL::Test::Utils::timeout_default)]);
$node_b->safe_psql($pgactive_test_dbname,
	qq[SELECT pgactive.pgactive_wait_for_node_ready($PostgreSQL::Test::Utils::timeout_default)]);

is($node_a->safe_psql($pgactive_test_dbname, 'SELECT pgactive.pgactive_is_active_in_db()'), 't',
	'pgactive is active on node_a');
is($node_b->safe_psql($pgactive_test_dbname, 'SELECT pgactive.pgactive_is_active_in_db()'), 't',
	'pgactive is active on node_b');

my $status_a = $node_a->safe_psql($pgactive_test_dbname, 'SELECT node_name, pgactive.pgactive_node_status_from_char(node_status) FROM pgactive.pgactive_nodes ORDER BY node_name');
my $status_b = $node_b->safe_psql($pgactive_test_dbname, 'SELECT node_name, pgactive.pgactive_node_status_from_char(node_status) FROM pgactive.pgactive_nodes ORDER BY node_name');

is($status_a, "node-a|pgactive_NODE_STATUS_READY\nnode-b|pgactive_NODE_STATUS_READY", 'node A sees both nodes as ready');
is($status_b, "node-a|pgactive_NODE_STATUS_READY\nnode-b|pgactive_NODE_STATUS_READY", 'node B sees both nodes as ready');

note "Taking ddl lock manually";

$node_a->safe_psql($pgactive_test_dbname, "SELECT pgactive.pgactive_acquire_global_lock('write_lock')");

note "Creating a table...";

exec_ddl($node_b, q[
CREATE TABLE public.reptest(
	id integer primary key,
	dummy text
);
]);

$node_a->poll_query_until($pgactive_test_dbname, q{
SELECT EXISTS (
  SELECT 1 FROM pg_class c INNER JOIN pg_namespace n ON n.nspname = 'public' AND c.relname = 'reptest'
);
});

ok($node_b->safe_psql($pgactive_test_dbname, "SELECT 'reptest'::regclass"), "reptest table creation replicated");

$node_a->safe_psql($pgactive_test_dbname, "INSERT INTO reptest (id, dummy) VALUES (1, '42')");

$node_b->poll_query_until($pgactive_test_dbname, q{
SELECT EXISTS (
  SELECT 1 FROM reptest
);
});

is($node_b->safe_psql($pgactive_test_dbname, 'SELECT id, dummy FROM reptest;'), '1|42', "reptest insert successfully replicated");

my $seqid_a = $node_a->safe_psql($pgactive_test_dbname, 'SELECT node_seq_id FROM pgactive.pgactive_nodes WHERE node_name = pgactive.pgactive_get_local_node_name()');
my $seqid_b = $node_b->safe_psql($pgactive_test_dbname, 'SELECT node_seq_id FROM pgactive.pgactive_nodes WHERE node_name = pgactive.pgactive_get_local_node_name()');

is($seqid_a, 1, 'first node got global sequence ID 1');
is($seqid_b, 2, 'second node got global sequence ID 2');

done_testing();
