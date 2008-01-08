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
    my $indiv=0;
    print "$f\n";

    open(TH, ">.tmp") or die "cannot open .tmp: $!\n";

    open(FH, $f) or die "cannot open $f: $!\n";
    while (<FH>) {
        if (/^<div class="tabs">$/) {
            $indiv=1;
            next;
        }
        if ($indiv) {
            if (/\<\/div\>/) {
                $indiv=0;
                next;
            }
            else {
                next;
            }
        }
        s/href=\"(.*?)\"/href=\"api\/module\/$1\"/g;
        print TH $_;
    }
    close(FH);
    close(TH);

    `cp .tmp $f`;
}
