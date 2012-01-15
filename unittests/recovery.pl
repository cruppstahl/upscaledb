
$max = 100;

sub check {
    my $status=shift;
    exit -1 unless $status == 0;
}

sub simple_test {
    for ($i=1; $i<=7; $i++) {
        unlink("recovery.db");
        unlink("recovery.db.log0");
        unlink("recovery.db.jrn0");
        unlink("recovery.db.jrn1");
    
        print "inserting $max keys...\n";
        for ($k=0; $k<$max; $k++) {
            check(system("./recovery insert 8 8 $k 0 $i"));
            #`cp recovery.db rec2.db`;
            #`cp recovery.db.log0 rec2.db.log0`;
            #`cp recovery.db.jrn0 rec2.db.jrn0`;
            #`cp recovery.db.jrn1 rec2.db.jrn1`;
            check(system("./recovery recover"));
            check(system("./recovery verify 8 8 $k 0 1"));
        }
    
        print "erasing $max keys...\n";
        for ($k=$max-1; $k>=0; $k--) {
            check(system("./recovery erase 8 $k 0 $i"));
            check(system("./recovery recover"));
            check(system("./recovery verify 8 8 $k 0 0"));
        }
    }
}

sub extended_test {
    for ($i=1; $i<=10; $i++) {
        unlink("recovery.db");
        unlink("recovery.db.log0");
        unlink("recovery.db.jrn0");
        unlink("recovery.db.jrn1");
    
        print "inserting $max keys...\n";
        for ($k=0; $k<$max; $k++) {
            check(system("./recovery insert 1024 1024 $k 0 $i"));
            #`cp recovery.db rec2.db`;
            #`cp recovery.db.log0 rec2.db.log0`;
            #`cp recovery.db.jrn0 rec2.db.jrn0`;
            #`cp recovery.db.jrn1 rec2.db.jrn1`;
            check(system("./recovery recover"));
            check(system("./recovery verify 1024 1024 $k 0 1"));
        }
    
        print "erasing $max keys...\n";
        for ($k=$max-1; $k>=0; $k--) {
            check(system("./recovery erase 1024 $k 0 $i"));
            check(system("./recovery recover"));
            check(system("./recovery verify 1024 1024 $k 0 0"));
        }
    }
}

sub duplicate_test {
    for ($i=1; $i<=7; $i++) {
        unlink("recovery.db");
        unlink("recovery.db.log0");
        unlink("recovery.db.jrn0");
        unlink("recovery.db.jrn1");
    
        print "inserting $max keys...\n";
        for ($k=0; $k<$max; $k++) {
            check(system("./recovery insert 8 8 $k 1 $i"));
            #`cp recovery.db rec2.db`;
            #`cp recovery.db.log0 rec2.db.log0`;
            #`cp recovery.db.jrn0 rec2.db.jrn0`;
            #`cp recovery.db.jrn1 rec2.db.jrn1`;
            check(system("./recovery recover"));
            check(system("./recovery verify 8 8 $k 1 1"));
        }
    
        print "erasing $max keys...\n";
        for ($k=$max-1; $k>=0; $k--) {
            check(system("./recovery erase 8 $k 1 $i"));
            check(system("./recovery recover"));
            check(system("./recovery verify 8 8 $k 1 0"));
        }
    }
}

sub extended_duplicate_test {
    for ($i=1; $i<=10; $i++) {
        unlink("recovery.db");
        unlink("recovery.db.log0");
        unlink("recovery.db.jrn0");
        unlink("recovery.db.jrn1");
    
        print "inserting $max keys...\n";
        for ($k=0; $k<$max; $k++) {
            check(system("./recovery insert 1024 1024 $k 1 $i"));
            #`cp recovery.db rec2.db`;
            #`cp recovery.db.log0 rec2.db.log0`;
            #`cp recovery.db.jrn0 rec2.db.jrn0`;
            #`cp recovery.db.jrn1 rec2.db.jrn1`;
            check(system("./recovery recover"));
            check(system("./recovery verify 1024 1024 $k 1 1"));
        }
    
        print "erasing $max keys...\n";
        for ($k=$max-1; $k>=0; $k--) {
            check(system("./recovery erase 1024 $k 1 $i"));
            check(system("./recovery recover"));
            check(system("./recovery verify 1024 1024 $k 1 0"));
        }
    }
}

print "----------------------------\nsimple_test\n";
simple_test();
print "----------------------------\nextended_test\n";
extended_test();
print "----------------------------\nduplicate_test\n";
duplicate_test();
print "----------------------------\nextended_duplicate_test\n";
extended_duplicate_test();
