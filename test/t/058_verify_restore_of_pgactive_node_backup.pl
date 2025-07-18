#!/usr/bin/env perl
#
# This test verifies that the instance restored from backup of pgactive node
# doesn't try to connect to upstream node, in other words, join pgactive group.
#
use strict;
use warnings;
use File::Path qw(rmtree);
use lib 'test/t/';
use Cwd;
use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use IPC::Run;
use Test::More;
use utils::nodemanagement;

# Create an upstream node and bring up pgactive
my $node_0 = PostgreSQL::Test::Cluster->new('node_0');
initandstart_node($node_0);
create_pgactive_group($node_0);

# Join a new node to the pgactive group
my $node_1 = PostgreSQL::Test::Cluster->new('node_1');
initandstart_node($node_1, $pgactive_test_dbname, extra_init_opts => { has_archiving => 1 });
pgactive_logical_join( $node_1, $node_0 );
check_join_status( $node_1, $node_0);

# Let's take a backup of pgactive node
my $backup_name = 'mybackup';
$node_1->backup($backup_name);

my $node_2 = PostgreSQL::Test::Cluster->new('node_2');
$node_2->init_from_backup($node_1, $backup_name);
$node_2->append_conf(
	'postgresql.conf', qq(
		log_min_messages = debug1
));
$node_2->start;

my $logstart_2 = get_log_size($node_2);

# Detached node must unregister per-db worker
my $result = find_in_log($node_2,
	qr!LOG: ( [A-Z0-9]+:)? unregistering per-db worker on node .* due to failure when connecting to ourself!,
	$logstart_2);
ok($result, "unregistering per-db worker due to failure when connecting to ourself is detected");

# Set the supervisor latch via a config file reload so that it detects the
# above unregistered per-db worker.
$node_2->reload;
$result = find_in_log($node_2,
	qr!DEBUG: ( [A-Z0-9]+:)? per-db worker for database with OID .* was previously unregistered, not registering!,
	$logstart_2);
ok($result, "previously unregistered per-db worker is detected");

# There mustn't be any pgactive workers on restored instance
$result = $node_2->safe_psql($pgactive_test_dbname, qq[SELECT COUNT(*) FROM pgactive.pgactive_get_workers_info() WHERE unregistered = false;]);
is($result, '0', "restored node " . $node_2->name() . " doesn't have pgactive workers");

# Let's get rid of pgactive completely on restored instance
$node_2->safe_psql($pgactive_test_dbname, qq[SELECT pgactive.pgactive_remove(true);]);
$node_2->safe_psql($pgactive_test_dbname, qq[DROP EXTENSION pgactive;]);

# Stop a node and remove the replication slot
my $logstart_1 = get_log_size($node_1);
my $datadir           = $node_1->data_dir;
my $slot_name = $node_1->safe_psql('postgres',
	"SELECT slot_name from pg_replication_slots;"
);
my $pgactive_replslotdir = "$datadir/pg_replslot/$slot_name";
$node_1->stop;
rmtree($pgactive_replslotdir);
$node_1->start;

# apply worker should not be started
$result = find_in_log($node_1,
	qr!LOG: ( [A-Z0-9]+:)? slot .* does not exist for node .*, skipping related apply worker start!,
	$logstart_1);
ok($result, "skipping related apply worker start due to missing replication slot");

# Create a node with long name (i.e > NAMEDATALEN bytes) and bring up pgactive
my $node_ln1 = PostgreSQL::Test::Cluster->new('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_1');
initandstart_node($node_ln1);

$node_ln1->safe_psql($pgactive_test_dbname,
    q[CREATE TABLE fruits(id integer, name varchar);]);
$node_ln1->safe_psql($pgactive_test_dbname,
    q[INSERT INTO fruits VALUES (1, 'Cherry');]);

create_pgactive_group($node_ln1);

# Join a node with long name (i.e > NAMEDATALEN bytes) to the pgactive group
my $node_ln2 = PostgreSQL::Test::Cluster->new('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_2');
initandstart_node($node_ln2, $pgactive_test_dbname);
pgactive_logical_join($node_ln2, $node_ln1);

$node_ln1->safe_psql($pgactive_test_dbname,
    q[INSERT INTO fruits VALUES (2, 'Apple');]);
wait_for_apply($node_ln1, $node_ln2);

$node_ln2->safe_psql($pgactive_test_dbname,
    q[INSERT INTO fruits VALUES (3, 'Mango');]);
wait_for_apply($node_ln2, $node_ln1);

# Check data is available on all pgactive nodes after join with long node
# names (i.e > NAMEDATALEN bytes).
my $expected = 3;
my $query = qq[SELECT COUNT(*) FROM fruits;];

my $res1 = $node_ln1->safe_psql($pgactive_test_dbname, $query);
my $res2 = $node_ln2->safe_psql($pgactive_test_dbname, $query);

is($res1, $expected, "node_ln1 with long node name has all the data");
is($res1, $expected, "node_ln2 with long node name has all the data");

$node_ln1->stop;
$node_ln2->stop;

done_testing();
