#!/usr/bin/perl

use Test::More 'no_plan';
use Test::TempDir;

use ok 'KiokuDB';
use ok 'KiokuDB::Backend::JSPON';

use KiokuDB::Test;

foreach my $fmt ( qw(storable json), eval { require YAML::XS; 'yaml' } ) {
    run_all_fixtures( KiokuDB->connect("jspon:dir=" . tempdir, serializer => $fmt) );
}

run_all_fixtures( KiokuDB->connect("jspon:dir=" . tempdir, trie => 1) );
