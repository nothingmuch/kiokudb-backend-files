#!/usr/bin/perl

package KiokuDB::Backend::JSPON;
use Moose;

use namespace::clean -except => 'meta';

our $VERSION = "0.06";

extends qw(KiokuDB::Backend::Files);

has '+serializer' => ( default => "json" );

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__

=pod

=head1 NAME

KiokuDB::Backend::JSPON - JSON file backend with JSPON
reference semantics

=head1 DESCRIPTION

This is just the L<KiokuDB::Backend::Files> backend with the serializer default
set to C<json> for backwards compatibility.

L<http://www.jspon.org/|JSPON> is a standard for encoding object graphs in
JSON.

The representation is based on explicit ID based references, and so is simple
enough to be stored in JSON.

=head1 TODO

=over 4

=item *

Refactor into FS role and general JSPON role, and implement a REST based
backend too

=back

=cut
