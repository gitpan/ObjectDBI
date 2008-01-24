#!/usr/bin/perl -I../lib

use ObjectDBI;
use Data::Dumper;

print "1..1\n";

my $objectdbi = ObjectDBI->new(
  dbiuri => 'DBI:Pg:dbname=test'
) || die "Could not connect to db";
my $n=0;
my $cursor = $objectdbi->cursor("foo");
while (my $ref = $cursor->next()) {
  ++$n;
}
if ($n) {
  print "ok 1\n";
} else {
  print "not ok 1\n";
}

1;
