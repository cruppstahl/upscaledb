#!/usr/bin/env python

# a test generator

import os
import sys
import random

# number of operations (=inserts), can be overwritten in argv[1]
conf_count=1000;
# use numeric keys?
conf_numeric_keys=1
# use variable key length?
conf_variable_keys=1
# maximum key length
conf_max_keylength=128
# use variable data size
conf_variable_data=1
# maximum data length
conf_max_datalength=1024

def generate_string(len):
    i=0
    s=''
    while i<len:
        i+=1
        s+=chr(random.randint(ord('A'), ord('z')))
    return s

try:
    conf_count=int(sys.argv[1])
except:
    pass

print "-- Configuration: "
print "--   conf_count =", conf_count
print "--   conf_numeric_keys =", conf_numeric_keys
print "--   conf_variable_keys =", conf_variable_keys
print "--   conf_max_keylength =", conf_max_keylength
print "--   conf_variable_data =", conf_variable_data
print "--   conf_max_datalength =", conf_max_datalength

if conf_numeric_keys:
    print "CREATE (NUMERIC_KEY)"
else:
    print "CREATE"

i=0
key=[]
while i<conf_count:
    i+=1
    if conf_numeric_keys:
        k=random.randint(0, conf_max_keylength)+1;
    else:
        if conf_variable_keys:
            k=generate_string(random.randint(0, conf_max_keylength)+1);
        else:
            k=generate_string(conf_max_keylength);
    if conf_variable_data:
        d=random.randint(0, conf_max_datalength)+1
    else:
        d=conf_max_datalength
    print "INSERT (0, \""+str(k)+"\", "+str(d)+")"
    key.append(k)

for k in key:
    print "ERASE  (0, \""+str(k)+"\")"

print "FULLCHECK"
print "CLOSE"

