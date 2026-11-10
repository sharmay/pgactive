#!/usr/bin/env perl
#
# Test that pgactive refuses to connect with a libpq connection option that
# isn't known to be safe (CVE-2026-14661).
use strict;
use warnings;
use lib 'test/t/';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use utils::nodemanagement;

my $node = PostgreSQL::Test::Cluster->new('node_a');
initandstart_node($node);

# Every connection option of libpq before 18 is allowed.
my $pg_version = $node->safe_psql($pgactive_test_dbname,
    'SHOW server_version_num');
if ($pg_version < 180000)
{
    $node->stop;
    plan skip_all => 'libpq before 18 has no disallowed connection options';
}

my $pgport = $node->port;
my $pghost = $node->host;
my $node_dsn = "port=$pgport host=$pghost dbname=$pgactive_test_dbname";
my $keylog = $node->basedir . '/keylog';

# Run pgactive_create_group, which must fail, and return its stderr.
sub create_group_stderr
{
    my ($dsn) = @_;
    my ($ret, $stdout, $stderr) = $node->psql(
        $pgactive_test_dbname, qq{
            SELECT pgactive.pgactive_create_group(
                    node_name := 'node_a',
                    node_dsn := '$dsn');});
    isnt($ret, 0, 'pgactive_create_group failed');
    return $stderr;
}

# A disallowed option in node_dsn.
like(create_group_stderr("$node_dsn sslkeylogfile=$keylog"),
     qr/ERROR:  permission denied to set connection option "sslkeylogfile"/,
     'sslkeylogfile in node_dsn is rejected');

# A disallowed option in a pgactive_fdw foreign server, which the FDW
# validator accepts.
my $user = $node->safe_psql($pgactive_test_dbname, 'SELECT current_user');
$node->safe_psql(
    $pgactive_test_dbname, qq{
        CREATE SERVER oauth_server FOREIGN DATA WRAPPER pgactive_fdw
            OPTIONS (port '$pgport', host '$pghost', dbname '$pgactive_test_dbname',
                     oauth_issuer 'https://localhost', oauth_client_id 'pgactive');
        CREATE USER MAPPING FOR "$user" SERVER oauth_server OPTIONS (user '$user');});
like(create_group_stderr("user_mapping=$user pgactive_foreign_server=oauth_server"),
     qr/ERROR:  permission denied to set connection option "oauth_issuer"/,
     'oauth_issuer in a pgactive_fdw foreign server is rejected');

# A disallowed option in pgactive.extra_apply_connection_options.
$node->append_conf('postgresql.conf',
    "pgactive.extra_apply_connection_options = 'sslkeylogfile=$keylog'");
$node->restart;
like(create_group_stderr($node_dsn),
     qr/ERROR:  permission denied to set connection option "sslkeylogfile"/,
     'sslkeylogfile in pgactive.extra_apply_connection_options is rejected');

$node->append_conf('postgresql.conf',
    "pgactive.extra_apply_connection_options = ''");
$node->restart;

# The options pgactive adds itself are allowed.
create_pgactive_group($node);
pass('pgactive_create_group succeeds with allowed options');

$node->stop;

done_testing();
