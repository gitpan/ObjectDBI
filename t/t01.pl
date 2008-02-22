#!/usr/bin/perl -I../lib

use ObjectDBI;
use Data::Dumper;

sub no_test {
  print "1..1\nok 1 Skipped # SKIP No database available\n";
  exit;
}

eval("use SQL::Statement;1;") || no_test();
my $objectdbi = eval {
  ObjectDBI->new(dbiuri => 'dbi:DBM:mldbm=Storable')
} || no_test();

$objectdbi->get_dbh()->do("
  create table perlobjects (
    obj_id text,
    obj_pid text,
    obj_gpid text,
    obj_name text,
    obj_type text,
    obj_value text
  )
");

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

$objectdbi->get_dbh()->do("drop table perlobjects");

1;
