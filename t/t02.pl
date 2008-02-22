#!/usr/bin/perl -I../lib

use ObjectDBI;
use Data::Dumper;

sub no_test {
  print "1..1\nok 1 Skipped # SKIP No database available\n";
  exit;
}

my $objectdbi = eval{ ObjectDBI->new(
  dbiuri => 'dbi:Pg:dbname=perlobjects'
) } || no_test();

$objectdbi->get_dbh()->do("
  create sequence perlobjectseq;
");

$objectdbi->get_dbh()->do("
  create table perlobjects (
    obj_id integer unique not null,
    obj_pid integer references perlobjects (obj_id),
    obj_gpid integer references perlobjects (obj_id),
    obj_name varchar(255),
    obj_type varchar(64),
    obj_value varchar(255)
  );
");

print "1..1\n";

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

$objectdbi->get_dbh()->do("drop table perlobjects");

1;
