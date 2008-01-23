#!/usr/bin/perl -I../lib

use ObjectDBI;
use Data::Dumper;
my $objectdbi = ObjectDBI->new( 
  dbiuri => 'DBI:Pg:dbname=test'
) || die "Could not connect to db";
my $hash = { foo => [ 'bar' ] };
$hash->{'foobar'} = $hash->{foo};
print Dumper($hash);
my $id = $objectdbi->put($hash);
my $ref = $objectdbi->get($id); 
print Dumper($ref);

1;
