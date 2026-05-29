#!/usr/bin/env perl
#
# Test unregistering per-db/apply worker after detaching.
use strict;
use warnings;
use lib 'test/t/';
use Cwd;
use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use IPC::Run;
use Test::More;
use utils::nodemanagement;

# Create an upstream node and bring up pgactive
my $nodes = make_pgactive_group(2,'node_');
my ($node_0,$node_1) = @$nodes;

# Detach a node from 2 node cluster
note "Detach node_0 from 2 node cluster\n";
pgactive_detach_nodes([$node_0], $node_1);
check_detach_status([$node_0], $node_1);

my $logstart_0 = get_log_size($node_0);
my $logstart_1 = get_log_size($node_1);

# Detached node must have no apply workers running after detach.
# Poll worker state directly rather than relying on log messages which may
# not appear if the worker is killed before it can log.
ok($node_0->poll_query_until($pgactive_test_dbname,
	qq[SELECT COUNT(*) = 0 FROM pgactive.pgactive_get_workers_info() WHERE worker_type = 'apply';]),
	"apply worker on node_0 is gone after detach");

# Remove pgactive from the detached node
$node_0->safe_psql($pgactive_test_dbname, "select pgactive.pgactive_remove(true)");

# per-db worker must be gone on a node with pgactive removed.
# Poll worker state directly rather than relying on log messages.
ok($node_0->poll_query_until($pgactive_test_dbname,
	qq[SELECT COUNT(*) = 0 FROM pgactive.pgactive_get_workers_info() WHERE worker_type = 'per-db';]),
	"per-db worker on node_0 is gone after pgactive_remove");

# Remove pgactive from node and immediately drop the extension
$node_1->safe_psql($pgactive_test_dbname,
	q[
		SELECT pgactive.pgactive_remove(true);
		DROP EXTENSION pgactive;
	]);

# After pgactive_remove + DROP EXTENSION, all pgactive workers must be gone.
# Poll pg_stat_activity directly since pgactive catalog functions are no longer
# available after DROP EXTENSION.
ok($node_1->poll_query_until('postgres',
	qq[SELECT COUNT(*) = 0 FROM pg_stat_activity WHERE application_name LIKE 'pgactive:%' AND datname = '$pgactive_test_dbname';]),
	"all pgactive workers on node_1 are gone after pgactive_remove and DROP EXTENSION");

done_testing();
