#!/usr/bin/perl 

open(FH, $ARGV[0]) or die "Cannot read $ARGV[0]: $!";
while (<FH>) {
    if (/^int g_buildno=(\d+);/) {
        close(FH);
        open(FH, ">$ARGV[0]") or die "Cannot write $ARGV[0]: $!";
        print FH "int g_buildno=".($1+1).";\n";
        close(FH);
        exit;
    }
}
