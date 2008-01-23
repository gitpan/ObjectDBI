#!/usr/bin/perl -I../lib

use ObjectDBI;
use Data::Dumper;

my $objectdbi = ObjectDBI->new(
  dbiuri => 'DBI:Pg:dbname=test'
) || die "Could not connect to db";
my $cursor = $objectdbi->cursor("foo");
while (my $ref = $cursor->next()) {
  print Dumper($ref);
}

1;
