#!/usr/bin/env perl

#@compression=('none', 'zint32_simdcomp', 'zint32_varbyte', 
              #'zint32_groupvarint', 'zint32_streamvbyte',
              #'zint32_maskedvbyte', 'zint32_blockindex');
@compression=('zint32_simdcomp', 'zint32_groupvarint', 'zint32_maskedvbyte');
@limit=('50000000');
@records=('0');

for $c (@compression) {
  for $l (@limit) {
    for $r (@records) {
      print "\n";
      print "==========================================================\n";
      #run_test($c, $l, $r, 'zipfian insert', "./ham_bench --stop-ops=$l  --key=uint32 --key-compression=$c --recsize-fixed=$r --metrics=all --quiet --seed=1 --distribution=zipfian --cache=500000000");
      #`cp test-ham.db test-ham-$c-zipf.db`;
      `cp test-ham-$c-zipf.db test-ham.db`;

      for (my $i = 0; $i <= 9; $i++) {
        run_test($c, $l, $r, 'zipfian lookup', "./ham_bench --stop-ops=$l  --key=uint32 --key-compression=$c --recsize-fixed=$r --metrics=all --quiet --seed=1 --open --find-pct=100 --distribution=zipfian");
      }

      #run_test($c, $l, $r, 'sequential insert', "./ham_bench --stop-ops=$l  --key=uint32 --key-compression=$c --recsize-fixed=$r --metrics=all --quiet --seed=1 --distribution=ascending --cache=500000000");
      #`cp test-ham.db test-ham-$c-seq.db`;

      `cp test-ham-$c-seq.db test-ham.db`;
      for (my $i = 0; $i <= 9; $i++) {
        run_test($c, $l, $r, 'sequential lookup', "./ham_bench --stop-ops=$l  --key=uint32 --key-compression=$c --recsize-fixed=$r --metrics=all --quiet --seed=1 --open --find-pct=100 --distribution=ascending");
      }
    }
  }
}

sub run_test
{
  $c = shift;
  $l = shift;
  $r = shift;
  $title = shift;
  $cmd = shift;

  #print "$cmd";
  $metrics = `$cmd 2>&1`;
  #print "result: $? - $metrics\n";

  print "$c: $title ($l operations, record size $r)\n";
  foreach (split(/\n/, $metrics)) {
    if (/hamsterdb elapsed time \(sec\) +(.*)/) {
      print "      elapsed time      $1\n";
    }
    elsif (/hamsterdb insert_\#ops +(.*) \((.*?)\)/) {
      print "      insert ops/sec    $2\n";
    }
    elsif (/hamsterdb find_\#ops +(.*) \((.*?)\)/) {
      print "      find ops/sec      $2\n";
    }
    elsif (/hamsterdb filesize +(.*)/) {
      print "      filesize          $1\n";
    }
  }
}
