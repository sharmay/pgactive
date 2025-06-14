#!/usr/bin/env perl
#
# Shared test code that doesn't relate directly to simple
# pgactive node management.
#
package utils::concurrent;

use strict;
use warnings;
use Exporter;
use Cwd;
use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use utils::nodemanagement qw(
    :DEFAULT
    $pgactive_test_dbname
    copy_transform_postgresqlconf
    start_pgactive_init_copy
    wait_detach_completion
    );

use vars qw(@ISA @EXPORT @EXPORT_OK);
@ISA         = qw(Exporter);
@EXPORT      = qw(
    concurrent_joins
    concurrent_joins_logical_physical
    join_under_write_load
    concurrent_detach
    concurrent_join_detach
    pgbench_init
    pgbench_start
    concurrent_inserts
    );
@EXPORT_OK   = qw();

# Check if concurrent multinode join works
sub concurrent_joins {
    my $type          = shift;
    my $upstream_node = shift;
    my @nodes_array   = @_;

    if ( $type eq 'logical' ) {
        concurrent_joins_logical( $upstream_node, @nodes_array );
    }
    elsif ( $type eq 'physical' ) {
        concurrent_joins_physical( $upstream_node, @nodes_array );
    }
}

# Execute a set of queries concurrently on a list of nodes
#
# Takes a ref to an array of [node,query] arrayrefs and an optional timeout.
#
# Returns number of failed calls.
#
sub concurrent_safe_psql {
    my ($node_queries, $timeout) = @_;

    $timeout = 60 if (!$timeout);

    my @handles;
    foreach my $node_query (@$node_queries) {
        # We can't just use $node->safe_psql here, because it will block (say,
        # on a DDL lock) until the query returns. Instead we'll use IPC::Run::start
        # to run multiple psql sessions asynchronously, since PostgresNode doesn't
        # have a helper for this.
        #
        my ($node, $query) = @$node_query;
        my $timeout_exc = 'timed out running psql on node ' . $node->name;
        my ($stdout, $stderr) = ('','');
        my $handle = IPC::Run::start(
            [
                'psql', '-v', 'ON_ERROR_STOP=1', $node->connstr($pgactive_test_dbname), '-f', '-'
            ],
            '1>', \$stdout, '2>', \$stderr, '<', \$query,
            IPC::Run::timeout($timeout, exception => $timeout_exc)
        );
        push @handles, [$handle,$node,$query,\$stdout,\$stderr];
    }

    my $failures = 0;
    foreach my $elem (@handles) {
        my ($handle, $node, $query, $stdout, $stderr) = @$elem;
        # Wait for all queries to complete and psql sessions to exit, checking
        # exit codes. We don't need to do the fancy interpretation safe_psql
        # does.
        $handle->finish;
        if (!is($handle->full_result(0), 0, "psql on node " . $node->name . " exited normally"))
        {
            $failures ++;
            diag "psql exit code: " . ($handle->result(0)) . " or signal: " . ($handle->full_result(0) & 127);
            diag "Query was: " . $query;
            diag "Stdout:\n---\n$$stdout\n---\nStderr:\n----\n$$stderr\n---";
        }
    }

    return $failures;
}

#
# Run multiple node joins using pgactive.pgactive_join_group concurrently,
# returning when all are complete.
#
# The concurrent equivalent of initandstart_logicaljoin_node .
#
sub concurrent_joins_logical {
    my @nodes   = @_;

    foreach my $join_node (@nodes) {
        my $node = @{$join_node}[0];
        initandstart_node($node);
        BAIL_OUT("no pgactive extension found in db '$pgactive_test_dbname'")
            if ($node->safe_psql($pgactive_test_dbname, "select 1 from pg_extension where extname = 'pgactive'") ne 1);
    }

    my @node_queries;

    foreach my $join_node (@nodes) {
        my $node = @{$join_node}[0];
        my $upstream_node = @{$join_node}[1];
        my $join_query = generate_pgactive_logical_join_query($node, $upstream_node);
        push @node_queries, [$node, $join_query];
    }

    if (concurrent_safe_psql(\@node_queries) > 0) {
        BAIL_OUT("one or more node join queries failed to execute");
    }

    # Now we have to wait for the nodes to actually join...
    foreach my $join_node (@nodes) {
        my $node = @{$join_node}[0];
        $node->safe_psql( $pgactive_test_dbname,
            qq[SELECT pgactive.pgactive_wait_for_node_ready($PostgreSQL::Test::Utils::timeout_default)]);
    }

    # and verify
    foreach my $join_node (@nodes) {
        my $node = @{$join_node}[0];
        my $upstream_node = @{$join_node}[1];
        check_join_status( $node, $upstream_node );
    }
}

#
# Run multiple node joins using pgactive_init_copy concurrently,
# returning when all are complete.
#
# The concurrent equivalent of initandstart_physicaljoin_node .
#
sub concurrent_joins_physical {
    my @nodes   = @_;
    my @handles;

    # Start pgactive_init_copy for each node we're asked to join.
    foreach my $join_node (@nodes) {
        my $node = @{$join_node}[0];
        my $upstream_node = @{$join_node}[1];
        my $new_conf_file = copy_transform_postgresqlconf( $node, $upstream_node );
        my $timeout = IPC::Run::timeout($PostgreSQL::Test::Utils::timeout_default, exception=>"Timed out");
        my $handle = start_pgactive_init_copy($node, $upstream_node, $new_conf_file, [$timeout]);
        push @handles, [$handle,$node];
    }

    # Wait until all the processes exit. 
    foreach my $elem (@handles) {
        my ($handle, $node) = @$elem;
        $handle->finish;
        # Did it exit normally?
        #
        # Return value here is that of $!, see "perldoc perlvar"
        is($handle->full_result(0), 0, "pgactive_init_copy for node " . $node->name . " started ok");
    }

    # wait for Pg to come up
    my $timeout = 60;
    foreach my $join_node (@nodes) {
        my $node = @{$join_node}[0];
        is(wait_for_pg_isready($node, $timeout),
                1, "node " . $node->name . " came up within $timeout seconds");
    }

    # wait for pgactive to come up
    foreach my $join_node (@nodes) {
        my $node = @{$join_node}[0];
        $node->safe_psql( $pgactive_test_dbname,
            qq[SELECT pgactive.pgactive_wait_for_node_ready($PostgreSQL::Test::Utils::timeout_default)]);
        $node->_update_pid(1);
    }

    # and validate
    foreach my $join_node (@nodes) {
        my $node = @{$join_node}[0];
        my $upstream_node = @{$join_node}[1];
        check_join_status( $node, $upstream_node );
    }
}

sub concurrent_detach {
    my @nodes   = @_;
    my @node_queries;

    foreach my $detach_node (@nodes) {
        my $node = @{$detach_node}[0];
        my $upstream_node = @{$detach_node}[1];
        my $detach_query = "SELECT pgactive.pgactive_detach_nodes(ARRAY['" . $node->name . "']);";
        push @node_queries, [$upstream_node, $detach_query];
    }

    if (concurrent_safe_psql(\@node_queries) > 0) {
        BAIL_OUT("one or more node detach queries failed to execute");
    }

    foreach my $detach_node (@nodes) {
        my $node = @{$detach_node}[0];
        my $upstream_node = @{$detach_node}[1];
        wait_detach_completion($node, $upstream_node);
    }
    
    foreach my $detach_node (@nodes) {
        push my @pgactive_detach_nodes,@{$detach_node}[0];
        my $upstream_node = @{$detach_node}[1];
        check_detach_status(\@pgactive_detach_nodes, $upstream_node);
    }
}

sub pgbench_init {
    my ($node, $scale) = @_;
    $node->pgbench(
	    "--initialize --scale=$scale", 0, [], [], 'pgbench init');
}

sub pgbench_start {
    my ($node, %kwargs) = @_;

    my @cmd = ('pgbench',);
    
    push @cmd, '-T', $kwargs{'time'} if defined $kwargs{'time'};
    push @cmd, '-c', $kwargs{'clients'} if defined $kwargs{'clients'};
    push @cmd, '-s', $kwargs{'scale'} if defined $kwargs{'scale'};

    push @cmd, '-d', $node->connstr($pgactive_test_dbname);

    my ($stdout, $stderr) = ('','');
    return IPC::Run::start( [@cmd], '2>', \$stderr, '>', \$stdout );
}

# Try to join a new node to upstream node
# while upstream is under write load
#
# pgbench init must have already been run.
#
sub join_under_write_load {
    my ($type, $upstream_node, $node, $pgbench_scale) = @_;
    $pgbench_scale = 10 if !defined $pgbench_scale;

    # Initiate heavy Inserts on upstream and simultaneously
    # try to join new node to the cluster
    my $pgbench_handle = pgbench_start($upstream_node, clients=>10, time=>10, scale=>$pgbench_scale);

    # and join the node while under load. (We could do concurrent
    # joins here too, same approach).
    if ( $type eq 'logical' ) {
        initandstart_logicaljoin_node($node, $upstream_node);
    }
    elsif ( $type eq 'physical' ) {
        initandstart_physicaljoin_node($node, $upstream_node);
    }

    $pgbench_handle->signal('TERM');
    $pgbench_handle->finish;
}

# Check if concurrent  join and detach works
sub concurrent_join_detach {
    my $type          = shift;
    my $upstream_node = shift;
    my ($join_nodes_array,$detach_nodes_array)   = @_;
    if ( $type eq 'logical' ) {
        concurrent_join_detach_logical( $upstream_node, \@{$join_nodes_array},\@{$detach_nodes_array} );
    }
    elsif ( $type eq 'physical' ) {
        concurrent_join_detach_physical( $upstream_node, \@{$join_nodes_array},\@{$detach_nodes_array} );
    }
}

sub concurrent_join_detach_logical {
    my $upstream_node = shift;
    my ($join_nodes,$pgactive_detach_nodes)   = @_;
    my @node_queries;
    
    # Collect all queries required to be executed concurrently
    foreach my $node (@{$pgactive_detach_nodes}) {
        my $detach_query = "SELECT pgactive.pgactive_detach_nodes(ARRAY['" . $node->name . "']);";
        push @node_queries, [$upstream_node, $detach_query];
    }
    foreach my $node (@{$join_nodes}) {
        initandstart_node($node);
        BAIL_OUT("no pgactive extension found in db '$pgactive_test_dbname'")
            if ($node->safe_psql($pgactive_test_dbname, "select 1 from pg_extension where extname = 'pgactive'") ne 1);
    }
    foreach my $node (@{$join_nodes}) {
        my $join_query = generate_pgactive_logical_join_query($node, $upstream_node);
        push @node_queries, [$node, $join_query];
    }
    
    #  Now execute the queries concurrently
    if (concurrent_safe_psql(\@node_queries) > 0) {
        BAIL_OUT("one or more node join queries failed to execute");
    }
    # Wait for detach completion and verify
    foreach my $node (@{$pgactive_detach_nodes}) {
        wait_detach_completion($node, $upstream_node);
    }
    check_detach_status(\@{$pgactive_detach_nodes}, $upstream_node);
    
    # Now we have to wait for the nodes to actually join...
    foreach my $node (@{$join_nodes}) {
        $node->safe_psql( $pgactive_test_dbname,
            qq[SELECT pgactive.pgactive_wait_for_node_ready($PostgreSQL::Test::Utils::timeout_default)]);
    }

    # and verify
    foreach my $node (@{$join_nodes}) {
        check_join_status( $node, $upstream_node );
    }
}

sub concurrent_join_detach_physical {
    my $upstream_node = shift;
    my ($join_nodes,$pgactive_detach_nodes)   = @_;
    my @node_queries;
    my @handles;
    
    # Collect all queries/cmds required to be executed concurrently
    foreach my $node (@{$pgactive_detach_nodes}) {
        my $detach_query = "SELECT pgactive.pgactive_detach_nodes(ARRAY['" . $node->name . "']);";
        push @node_queries, [$upstream_node, $detach_query];
    }

    # Start pgactive_init_copy for each node we're asked to joini.
    foreach my $node (@{$join_nodes}) {
        my $new_conf_file = copy_transform_postgresqlconf( $node, $upstream_node );
        my $timeout = IPC::Run::timeout($PostgreSQL::Test::Utils::timeout_default, exception=>"Timed out");
        my $handle = start_pgactive_init_copy($node, $upstream_node, $new_conf_file, [$timeout]);
        push @handles, [$handle,$node];
    }

    #  Now execute the detach queries concurrently
    if (concurrent_safe_psql(\@node_queries) > 0) {
        BAIL_OUT("one or more node join/detach operations failed to execute");
    }
    
    # Wait until all the processes exit. 
    foreach my $elem (@handles) {
        my ($handle, $node) = @$elem;
        $handle->finish;
        # Did it exit normally?
        #
        # Return value here is that of $!, see "perldoc perlvar"
        is($handle->full_result(0), 0, "pgactive_init_copy for node " . $node->name . " started ok");
    }


    # Wait for detach completion and verify
    foreach my $node (@{$pgactive_detach_nodes}) {
        wait_detach_completion($node, $upstream_node);
    }

    # and validate
    check_detach_status(\@{$pgactive_detach_nodes}, $upstream_node);
    
    # wait for Pg to come up
    my $timeout = 60;
    foreach my $node (@{$join_nodes}) {
        is(wait_for_pg_isready($node, $timeout),
            1, "node " . $node->name . " came up within $timeout seconds");
    }

    # wait for pgactive to come up
    foreach my $node (@{$join_nodes}) {
        $node->safe_psql( $pgactive_test_dbname,
            qq[SELECT pgactive.pgactive_wait_for_node_ready($PostgreSQL::Test::Utils::timeout_default)]);
        $node->_update_pid(1);
    }

    # and validate
    foreach my $node (@{$join_nodes}) {
        check_join_status( $node, $upstream_node );
    }

}
sub concurrent_joins_logical_physical {
    my ($join_nodes_logical,$join_nodes_physical)   = @_;
    my @node_queries;
    my @handles;
    
    # Collect queries for al logical joins 
    foreach my $join_node (@{$join_nodes_logical}) {
        my $node = @{$join_node}[0];
        initandstart_node($node);
        BAIL_OUT("no pgactive extension found in db '$pgactive_test_dbname'")
            if ($node->safe_psql($pgactive_test_dbname, "select 1 from pg_extension where extname = 'pgactive'") ne 1);
    }

    foreach my $join_node (@{$join_nodes_logical}) {
        my $node = @{$join_node}[0];
        my $upstream_node = @{$join_node}[1];
        my $join_query = generate_pgactive_logical_join_query($node, $upstream_node);
        push @node_queries, [$node, $join_query];
    }

    # Start pgactive_init_copy for each node we're asked to join.
    foreach my $join_node (@{$join_nodes_physical}) {
        my $node = @{$join_node}[0];
        my $upstream_node = @{$join_node}[1];
        my $new_conf_file = copy_transform_postgresqlconf( $node, $upstream_node );
        my $timeout = IPC::Run::timeout($PostgreSQL::Test::Utils::timeout_default, exception=>"Timed out");
        my $handle = start_pgactive_init_copy($node, $upstream_node, $new_conf_file, [$timeout]);
        push @handles, [$handle,$node];
    }
    
    # Start logical joins
    if (concurrent_safe_psql(\@node_queries) > 0) {
        BAIL_OUT("one or more node join queries failed to execute");
    }

    # Wait until all the processes exit. 
    foreach my $elem (@handles) {
        my ($handle, $node) = @$elem;
        $handle->finish;
        # Did it exit normally?
        #
        # Return value here is that of $!, see "perldoc perlvar"
        is($handle->full_result(0), 0, "pgactive_init_copy for node " . $node->name . " started ok");
    }

    # wait for Pg to come up
    my $timeout = 60;
    foreach my $join_node (@{$join_nodes_physical}) {
        my $node = @{$join_node}[0];
        is(wait_for_pg_isready($node, $timeout),
            1, "node " . $node->name . " came up within $timeout seconds");
    }
    
    my @join_nodes;
    push @join_nodes , @{$join_nodes_physical} , @{$join_nodes_logical};
    # wait for pgactive to come up
    foreach my $join_node (@join_nodes) {
        my $node = @{$join_node}[0];
        $node->safe_psql( $pgactive_test_dbname,
            qq[SELECT pgactive.pgactive_wait_for_node_ready($PostgreSQL::Test::Utils::timeout_default)]);
        $node->_update_pid(1);
    }

    # and validate
    foreach my $join_node (@join_nodes) {
        my $node = @{$join_node}[0];
        my $upstream_node = @{$join_node}[1];
        check_join_status( $node, $upstream_node );
    }

}
# Do concurrent inserts into table_with_sequence
# from 2 or more nodes. Can be called for inserts into 
# single node.
sub concurrent_inserts {
    my ($upstream_node,$table_name,$inserts,@nodes) = @_;
    my @node_queries;
    $upstream_node->safe_psql( $pgactive_test_dbname,
        qq[SELECT pgactive.pgactive_wait_for_node_ready($PostgreSQL::Test::Utils::timeout_default)]);
    $upstream_node->safe_psql($pgactive_test_dbname,"TRUNCATE TABLE $table_name");
    foreach my $node (@nodes) {
        my $node_name = $node->name();
        $node->safe_psql( $pgactive_test_dbname,
            qq[SELECT pgactive.pgactive_wait_for_node_ready($PostgreSQL::Test::Utils::timeout_default)]);
        my $insert_query = "INSERT INTO public.$table_name(node_name) SELECT '$node_name' FROM generate_series(1,$inserts)";
        push @node_queries, [$node, $insert_query];
    }
    if (concurrent_safe_psql(\@node_queries) > 0) {
        BAIL_OUT("one or more node insert queries failed to execute");
    }
}
1;
