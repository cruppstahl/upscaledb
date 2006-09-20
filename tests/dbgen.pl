#!/usr/bin/perl

#
# script for generating tests with variable length keys
#
# (C) Christoph Rupp, chris@crupp.de
# Di Sep 19 13:02:31 GMT 2006
#


$count=150;

sub generate_key
{
    $l=shift;
    $k='';
    $s=rand(20);

    for ($j=$s; $j<$l+$s; $j++) {
        $k.=chr(ord('a')+($j%20));
    }
    return $k;
}

print "CREATE\n";
for ($i=0; $i<$count; $i++) {
    $key=generate_key(int(rand(1000)));
    $len=int(rand(1000));
    print "INSERT (0, \"$key\", $len)\n";
    print "FULLCHECK\n" if ($i%20==0);
}
print "CLOSE\n";
