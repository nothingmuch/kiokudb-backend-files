#!/usr/bin/perl -w

use strict;

use ExtUtils::MakeMaker;

require 5.008; # utf8 and what not

WriteMakefile(
    NAME         => 'KiokuDB::Backend::JSPON',
    VERSION_FROM => 'lib/KiokuDB/Backend/JSPON.pm',
    INSTALLDIRS  => 'site',
    SIGN         => 1,
    PL_FILES     => { },
    PREREQ_PM    => {
        'Moose' => 0,
        'KiokuDB' => 0.05,

        'Test::TempDir' => 0,

        'JSON' => 0,
        'File::NFSLock' => 0,
        'IO::AtomicFile' => 0,
        'Path::Class' => 0,
        'MooseX::Types::Path::Class' => 0,
    },
);
