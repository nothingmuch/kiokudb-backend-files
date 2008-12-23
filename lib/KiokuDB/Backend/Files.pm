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

    if ( $self->create ) {
        $self->create_dirs;
    } else {
        my $dir = $self->dir;
        $dir->open || croak("$dir: $!");
    }
}

has dir => (
    isa => Dir,
    is  => "ro",
    required => 1,
    coerce   => 1,
);

has create => (
    isa => "Bool",
    is  => "ro",
    default => 0,
);

has object_dir => (
    isa => Dir,
    is  => "ro",
    lazy_build => 1,
);

sub _build_object_dir {
    my $self = shift;
    $self->dir->subdir("all");
}

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

=pod

=head1 NAME

KiokuDB::Backend::Files - One file per object backend

=head1 SYNOPSIS

    KiokuDB->connect(
        "files:dir=path/to/data",
        serializer => "yaml", # defaults to storable
    );

=head1 DESCRIPTION

This backend provides a file based backend, utilizing L<IO::AtomicFile> and
L<File::NFSLock> for safety.

This is one of the slower backends, and the support for searching is very
limited (only a linear scan is supported), but it is suitable for small, simple
projects.

=head1 ATTRIBUTES

=over 4

=item dir

The directory for the backend.

=item create

If true (defaults to false) the directories will be created at instantiation time.

=item lock

Whether or not locking is enabled.

Defaults to true.

=item object_dir

Defaults to the subdirectory C<all> of C<dir>

=item root_dir

Defaults to the subdirectory C<root> of C<dir>

Root set entries are hard linked into this directory as well.

=item trie

If true (defaults to false) instead of one flat hierarchy, the files will be
put in subdirectories based on their IDs. This is useful if your file system is
limited and you have lots of entries in the database.

=item trie_nybbles

How many hex nybbles to take off of the ID. Defaults to 3, which means up to
4096 subdirectories per directory.

=item trie_levels

How many subdirectories to use.

Defaults to 2.

=back

=head1 VERSION CONTROL

L<http://github.com/nothingmuch/kiokudb-backend-files>

=head1 AUTHOR

Yuval Kogman E<lt>nothingmuch@woobling.orgE<gt>

=head1 COPYRIGHT

    Copyright (c) 2008 Yuval Kogman, Infinity Interactive. All rights
    reserved This program is free software; you can redistribute
    it and/or modify it under the same terms as Perl itself.

=cut
