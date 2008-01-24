#!/usr/bin/perl -I../lib

use ObjectDBI;
use Data::Dumper;
my $objectdbi = ObjectDBI->new( 
  dbiuri => 'DBI:Pg:dbname=test'
) || die "Could not connect to db";
my $hash = { foo => [ 'bar' ] };
$hash->{'foobar'} = $hash->{foo};
my $str1 = Dumper($hash);
my $id = $objectdbi->put($hash);
my $ref = $objectdbi->get($id); 
my $str2 = Dumper($ref);
print "1..1\n";
if ($str1 eq $str2) {
  print "ok 1\n";
} else {
  print "not ok 1\n";
}

1;
