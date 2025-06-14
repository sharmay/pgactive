#!/usr/bin/perl
use strict;
use warnings;
use lib 'test/t/';
use PostgreSQL::Test::Cluster;
use Test::More;
use PostgreSQL::Test::Utils;
require 'common/pgactive_global_sequence.pl';

# This executes all the global sequence related tests
# for logical joins
global_sequence_tests('logical');

done_testing();
