#!/usr/bin/perl
#

my $rand=$ARGV[0];

while (1) {
    $sh=`./btree -r $rand`;
    $sh =~ /errors: (\d+)/;
    die $sh if $1;
    print "test passed\n";
    #sleep 5;
}
