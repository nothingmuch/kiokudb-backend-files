#!/usr/bin/perl

package KiokuDB::Backend::Files;
use Moose;

use Carp qw(croak);

use File::NFSLock;
use IO::AtomicFile;
use JSON;

use Data::Stream::Bulk::Path::Class;

use MooseX::Types::Path::Class qw(Dir File);

use namespace::clean -except => 'meta';

our $VERSION = "0.01";

with qw(
    KiokuDB::Backend
    KiokuDB::Backend::Serialize::Delegate
    KiokuDB::Backend::Role::Clear
    KiokuDB::Backend::Role::Scan
    KiokuDB::Backend::Role::Query::Simple::Linear
);

sub BUILD {
    my $self = shift;
    $self->create_dirs;
}

has dir => (
    isa => Dir,
    is  => "ro",
    required => 1,
    coerce   => 1,
);

has object_dir => (
    isa => Dir,
    is  => "ro",
    lazy_build => 1,
);

# TODO implement trie fanning on disk
has trie => (
    isa => "Bool",
    is  => "ro",
    default => 0,
);

# how many hex nybbles per trie level
has trie_nybbles => (
    isa => "Int",
    is  => "rw",
    default => 3, # default 4096 entries per level
);

# /dec/afb/decafbad
has trie_levels => (
    isa => "Int",
    is  => "rw",
    default => 2,
);

sub _build_object_dir {
    my $self = shift;
    $self->dir->subdir("all");
}

has root_set_dir => (
    isa => Dir,
    is  => "ro",
    lazy_build => 1,
);

sub _build_root_set_dir {
    my $self = shift;
    $self->dir->subdir("root");
}

has lock => (
    isa => "Bool",
    is  => "rw",
    default => 1,
);

has lock_file => (
    isa => File,
    is  => "ro",
    lazy_build => 1,
);

sub _build_lock_file {
    my $self = shift;
    $self->dir->file("lock");
}

sub write_lock {
    my $self = shift;

    return 1 unless $self->lock;

    File::NFSLock->new({ file => $self->lock_file->stringify, lock_type => "EXCLUSIVE" });
}

sub get {
    my ( $self, @uids ) = @_;

    return map { $self->get_entry($_) } @uids;
}

sub insert {
    my ( $self, @entries ) = @_;

    foreach my $entry ( @entries ) {
        $self->insert_entry($entry);
    }
}

sub delete {
    my ( $self, @ids_or_entries ) = @_;

    my @uids = map { ref($_) ? $_->id : $_ } @ids_or_entries;

    foreach my $uid ( @uids ) {
        foreach my $file ( $self->object_file($uid), $self->root_set_file($uid) ) {
            $file->remove;
        }
    }
}

sub exists {
    my ( $self, @uids ) = @_;

    map { -e $self->object_file($_) } @uids;
}

sub get_entry {
    my ( $self, $uid ) = @_;

    my $fh = $self->open_entry($uid);

    return $self->serializer->deserialize_from_stream($fh);
}

sub open_entry {
    my ( $self, $id ) = @_;

    $self->object_file($id)->openr;
}

sub insert_entry {
    my ( $self, $entry ) = @_;

    my $id = $entry->id;

    my $file = $self->object_file($id);

    $file->parent->mkpath unless -d $file->parent;

    my $fh = IO::AtomicFile->open( $file, "w" );

    $self->serializer->serialize_to_stream($fh, $entry);

    {
        my $lock = $self->write_lock;

        $fh->close || croak "Couldn't store: $!";

        my $root_file = $self->root_set_file($id);
        $root_file->remove;

        if ( $entry->root ) {
            $root_file->parent->mkpath unless -d $root_file->parent;
            link( $file, $root_file );
        }
    }
}

sub _trie_dir {
    my ( $self, $dir, $uid ) = @_;

    return $dir unless $self->trie;

    my $id_hex = unpack("H*", $uid);

    my $nyb = $self->trie_nybbles;

    for ( 1 .. $self->trie_levels ) {
        $dir = $dir->subdir( substr($id_hex, 0, $nyb, '') );
    }

    return $dir;
}

sub object_file {
    my ( $self, $uid ) = @_;

    my $dir = $self->_trie_dir( $self->object_dir, $uid);

    $dir->file($uid);
}

sub root_set_file {
    my ( $self, $uid ) = @_;

    my $dir = $self->_trie_dir( $self->root_set_dir, $uid);

    $dir->file($uid);
}

sub create_dirs {
    my $self = shift;

    $self->object_dir->mkpath;
    $self->root_set_dir->mkpath;
}

sub clear {
    my $self = shift;

    $_->rmtree({ keep_root => 1 }) for $self->root_set_dir, $self->object_dir;
}

sub all_entries {
    my $self = shift;

    my $ser = $self->serializer;

    Data::Stream::Bulk::Path::Class->new( dir => $self->object_dir, only_files => 1 )->filter(sub { [ map {
        $ser->deserialize_from_stream( $_->openr );
    } @$_ ]});
}

sub root_entries {
    my $self = shift;

    my $ser = $self->serializer;

    Data::Stream::Bulk::Path::Class->new( dir => $self->root_set_dir, only_files => 1 )->filter(sub { [ map {
        $ser->deserialize_from_stream( $_->openr );
    } @$_ ]});
}

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__
