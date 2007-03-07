#!/usr/bin/perl

opendir(DH, "html_www") or die "cannot open subdir: $!\n";
@f=readdir(DH);
closedir(DH);
foreach (@f) {
    if (/html$/) {
        process("html_www/$_");
    }
}

sub process
{
    my $f=shift;
    print "$f\n";

    open(TH, ">.tmp") or die "cannot open .tmp: $!\n";

    open(FH, $f) or die "cannot open $f: $!\n";
    while (<FH>) {
        s/href=\"(.*?)\"/href=\"?page=doxygen&module=$1\"/g;
        print TH $_;
    }
    close(FH);
    close(TH);

    `cp .tmp $f`;
}
