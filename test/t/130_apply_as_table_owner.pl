#!/usr/bin/env perl
#
# Test pgactive.apply_as_table_owner GUC.
#
# Verifies that when enabled (default), the apply worker executes DML
# (INSERT, UPDATE, DELETE) as the table owner rather than as superuser.
#
# The C code emits elog(DEBUG1, "pgactive apply <OP> as user <name> on <table>")
# when the GUC is on. We set pgactive.log_min_messages = debug1 on the receiving
# node and verify from the server log.
#
# Tests with DDL replication both on and off.
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

# Helper: read log file from offset, return content (for negative assertions)
sub log_content_from {
	my ($node, $offset) = @_;
	my $log = $node->logfile;
	open(my $fh, '<', $log) or die "Cannot open $log: $!";
	seek($fh, $offset, 0);
	my $content = do { local $/; <$fh> };
	close($fh);
	return $content;
}

##
## Part A: GUC on (default) - apply should run as table owner
##

my $nodes = make_pgactive_group(2, 'node_');
my ($node_0, $node_1) = @$nodes;

# Enable debug1 logging on node_1 so elog(DEBUG1) messages appear in apply worker log.
# pgactive bgworkers use pgactive.log_min_messages for their own log level.
$node_1->safe_psql($pgactive_test_dbname,
	q[ALTER SYSTEM SET pgactive.log_min_messages = debug1;]);
$node_1->safe_psql($pgactive_test_dbname,
	q[SELECT pg_reload_conf();]);
sleep(1);

# Create a non-superuser role and grant schema access via replicated DDL
exec_ddl($node_0, q[CREATE ROLE table_owner LOGIN;]);
exec_ddl($node_0, q[GRANT ALL ON SCHEMA public TO table_owner;]);
wait_for_apply($node_0, $node_1);

is($node_1->safe_psql($pgactive_test_dbname,
	q[SELECT 1 FROM pg_roles WHERE rolname = 'table_owner';]),
	'1', 'table_owner role exists on node_1');

# Create a table owned by table_owner
exec_ddl($node_0, q[CREATE TABLE public.owner_test(id integer primary key, data text);]);
exec_ddl($node_0, q[ALTER TABLE public.owner_test OWNER TO table_owner;]);
wait_for_apply($node_0, $node_1);

is($node_1->safe_psql($pgactive_test_dbname,
	q[SELECT tableowner FROM pg_tables WHERE tablename = 'owner_test';]),
	'table_owner', 'table ownership replicated to node_1');

# Confirm the GUC defaults to on
is($node_1->safe_psql($pgactive_test_dbname,
	q[SHOW pgactive.apply_as_table_owner;]),
	'on', 'GUC defaults to on');

my $logstart_1 = get_log_size($node_1);

# Test INSERT replication as table owner
$node_0->safe_psql($pgactive_test_dbname,
	q[INSERT INTO owner_test(id, data) VALUES (1, 'test1');]);
wait_for_apply($node_0, $node_1);

is($node_1->safe_psql($pgactive_test_dbname,
	q[SELECT data FROM owner_test WHERE id = 1;]),
	'test1', 'insert replicated with GUC on (default)');

ok(find_in_log($node_1, qr/pgactive apply INSERT as user table_owner on owner_test/, $logstart_1),
	'INSERT applied as table_owner (default)');

# Test UPDATE replication as table owner
$logstart_1 = get_log_size($node_1);

$node_0->safe_psql($pgactive_test_dbname,
	q[UPDATE owner_test SET data = 'updated1' WHERE id = 1;]);
wait_for_apply($node_0, $node_1);

is($node_1->safe_psql($pgactive_test_dbname,
	q[SELECT data FROM owner_test WHERE id = 1;]),
	'updated1', 'update replicated with GUC on (default)');

ok(find_in_log($node_1, qr/pgactive apply UPDATE as user table_owner on owner_test/, $logstart_1),
	'UPDATE applied as table_owner (default)');

# Test DELETE replication as table owner
$logstart_1 = get_log_size($node_1);

$node_0->safe_psql($pgactive_test_dbname,
	q[DELETE FROM owner_test WHERE id = 1;]);
wait_for_apply($node_0, $node_1);

is($node_1->safe_psql($pgactive_test_dbname,
	q[SELECT count(*) FROM owner_test WHERE id = 1;]),
	'0', 'delete replicated with GUC on (default)');

ok(find_in_log($node_1, qr/pgactive apply DELETE as user table_owner on owner_test/, $logstart_1),
	'DELETE applied as table_owner (default)');

##
## Part B: DDL replication OFF - verify apply_as_table_owner still works
##

# Turn off DDL replication on both nodes via restart
foreach my $node ($node_0, $node_1)
{
	$node->append_conf('postgresql.conf', "pgactive.skip_ddl_replication = on\n");
	$node->restart;
}

# Wait for pgactive to be ready after restart
foreach my $node ($node_0, $node_1)
{
	$node->safe_psql($pgactive_test_dbname,
		qq[SELECT pgactive.pgactive_wait_for_node_ready($PostgreSQL::Test::Utils::timeout_default)]);
}

$logstart_1 = get_log_size($node_1);

# DML should still replicate and apply as table owner
$node_0->safe_psql($pgactive_test_dbname,
	q[INSERT INTO owner_test(id, data) VALUES (3, 'ddl_off_test');]);
wait_for_apply($node_0, $node_1);

is($node_1->safe_psql($pgactive_test_dbname,
	q[SELECT data FROM owner_test WHERE id = 3;]),
	'ddl_off_test', 'insert replicated with DDL replication off');

ok(find_in_log($node_1, qr/pgactive apply INSERT as user table_owner on owner_test/, $logstart_1),
	'INSERT applied as table_owner with DDL replication off');

$logstart_1 = get_log_size($node_1);

$node_0->safe_psql($pgactive_test_dbname,
	q[UPDATE owner_test SET data = 'ddl_off_updated' WHERE id = 3;]);
wait_for_apply($node_0, $node_1);

ok(find_in_log($node_1, qr/pgactive apply UPDATE as user table_owner on owner_test/, $logstart_1),
	'UPDATE applied as table_owner with DDL replication off');

$logstart_1 = get_log_size($node_1);

$node_0->safe_psql($pgactive_test_dbname,
	q[DELETE FROM owner_test WHERE id = 3;]);
wait_for_apply($node_0, $node_1);

ok(find_in_log($node_1, qr/pgactive apply DELETE as user table_owner on owner_test/, $logstart_1),
	'DELETE applied as table_owner with DDL replication off');

##
## Part C: Disable GUC via restart - apply reverts to superuser
##

foreach my $node ($node_0, $node_1)
{
	$node->append_conf('postgresql.conf', "pgactive.apply_as_table_owner = off\n");
	$node->append_conf('postgresql.conf', "pgactive.skip_ddl_replication = off\n");
	$node->restart;
}

# Wait for pgactive to be ready after restart
foreach my $node ($node_0, $node_1)
{
	$node->safe_psql($pgactive_test_dbname,
		qq[SELECT pgactive.pgactive_wait_for_node_ready($PostgreSQL::Test::Utils::timeout_default)]);
}

is($node_1->safe_psql($pgactive_test_dbname,
	q[SHOW pgactive.apply_as_table_owner;]),
	'off', 'GUC is off after explicit disable');

$logstart_1 = get_log_size($node_1);

# Test INSERT with GUC disabled
$node_0->safe_psql($pgactive_test_dbname,
	q[INSERT INTO owner_test(id, data) VALUES (10, 'after_disable');]);
wait_for_apply($node_0, $node_1);

is($node_1->safe_psql($pgactive_test_dbname,
	q[SELECT data FROM owner_test WHERE id = 10;]),
	'after_disable', 'insert replicated after GUC disabled');

# Test UPDATE with GUC disabled
$node_0->safe_psql($pgactive_test_dbname,
	q[UPDATE owner_test SET data = 'disabled_update' WHERE id = 10;]);
wait_for_apply($node_0, $node_1);

is($node_1->safe_psql($pgactive_test_dbname,
	q[SELECT data FROM owner_test WHERE id = 10;]),
	'disabled_update', 'update replicated after GUC disabled');

# Test DELETE with GUC disabled
$node_0->safe_psql($pgactive_test_dbname,
	q[DELETE FROM owner_test WHERE id = 10;]);
wait_for_apply($node_0, $node_1);

is($node_1->safe_psql($pgactive_test_dbname,
	q[SELECT count(*) FROM owner_test WHERE id = 10;]),
	'0', 'delete replicated after GUC disabled');

# No apply-as-user messages (apply already completed, just read log)
my $log_content = log_content_from($node_1, $logstart_1);
ok($log_content !~ /pgactive apply INSERT as user .* on owner_test/,
	'after GUC disabled, no apply-as-user log for INSERT');
ok($log_content !~ /pgactive apply UPDATE as user .* on owner_test/,
	'after GUC disabled, no apply-as-user log for UPDATE');
ok($log_content !~ /pgactive apply DELETE as user .* on owner_test/,
	'after GUC disabled, no apply-as-user log for DELETE');

##
## Part D: Negative test - ownership divergence between nodes
##
## When table ownership differs between nodes, the apply worker should use
## the LOCAL table owner (from the receiving node), not the origin's owner.
## This tests that divergent ownership is handled correctly.
##

# Re-enable GUC for this test
foreach my $node ($node_0, $node_1)
{
	$node->append_conf('postgresql.conf', "pgactive.apply_as_table_owner = on\n");
	$node->restart;
}

foreach my $node ($node_0, $node_1)
{
	$node->safe_psql($pgactive_test_dbname,
		qq[SELECT pgactive.pgactive_wait_for_node_ready($PostgreSQL::Test::Utils::timeout_default)]);
}

# Create a second role on both nodes
exec_ddl($node_0, q[CREATE ROLE other_owner LOGIN;]);
exec_ddl($node_0, q[GRANT ALL ON SCHEMA public TO other_owner;]);
wait_for_apply($node_0, $node_1);

# Create a table with divergent ownership:
# - node_0 owns it as table_owner
# - node_1 will own it as other_owner
exec_ddl($node_0, q[CREATE TABLE public.divergent_test(id integer primary key, data text);]);
exec_ddl($node_0, q[ALTER TABLE public.divergent_test OWNER TO table_owner;]);
wait_for_apply($node_0, $node_1);

# Change ownership on node_1 only (skip DDL replication)
$node_1->safe_psql($pgactive_test_dbname, q[
	SET pgactive.skip_ddl_replication = true;
	ALTER TABLE public.divergent_test OWNER TO other_owner;
]);

# Verify ownership is now divergent
is($node_0->safe_psql($pgactive_test_dbname,
	q[SELECT tableowner FROM pg_tables WHERE tablename = 'divergent_test';]),
	'table_owner', 'divergent_test owned by table_owner on node_0');

is($node_1->safe_psql($pgactive_test_dbname,
	q[SELECT tableowner FROM pg_tables WHERE tablename = 'divergent_test';]),
	'other_owner', 'divergent_test owned by other_owner on node_1');

# INSERT from node_0 - on node_1 apply should run as other_owner (the LOCAL owner)
$logstart_1 = get_log_size($node_1);

$node_0->safe_psql($pgactive_test_dbname,
	q[INSERT INTO divergent_test(id, data) VALUES (1, 'divergent');]);
wait_for_apply($node_0, $node_1);

is($node_1->safe_psql($pgactive_test_dbname,
	q[SELECT data FROM divergent_test WHERE id = 1;]),
	'divergent', 'insert replicated with divergent ownership');

ok(find_in_log($node_1, qr/pgactive apply INSERT as user other_owner on divergent_test/, $logstart_1),
	'INSERT applied as LOCAL owner (other_owner) not origin owner (table_owner)');

# UPDATE from node_0 - should apply as other_owner on node_1
$logstart_1 = get_log_size($node_1);

$node_0->safe_psql($pgactive_test_dbname,
	q[UPDATE divergent_test SET data = 'divergent_updated' WHERE id = 1;]);
wait_for_apply($node_0, $node_1);

ok(find_in_log($node_1, qr/pgactive apply UPDATE as user other_owner on divergent_test/, $logstart_1),
	'UPDATE applied as LOCAL owner (other_owner) not origin owner (table_owner)');

# DELETE from node_0 - should apply as other_owner on node_1
$logstart_1 = get_log_size($node_1);

$node_0->safe_psql($pgactive_test_dbname,
	q[DELETE FROM divergent_test WHERE id = 1;]);
wait_for_apply($node_0, $node_1);

ok(find_in_log($node_1, qr/pgactive apply DELETE as user other_owner on divergent_test/, $logstart_1),
	'DELETE applied as LOCAL owner (other_owner) not origin owner (table_owner)');

done_testing();
