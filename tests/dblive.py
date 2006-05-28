#!/usr/bin/env python

# live test: runs till <strg>-C is pressed

import os
import stat
import sys
import random
import re
import popen2
import select

tstlog=open("tstlog.txt", "w")

def my_print(text):
    print text
    tstlog.write(text+"\n")

def run_live():
    i=0
    last=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    my_print("CREATE (NUMERIC_KEY)")
    while 1:
        if i%3==0:
            k=last[random.randint(0, 20)]
            if not k:
                k=1
            my_print("ERASE  (0, \""+str(k)+"\")")
        else:
            k=random.randint(0, 1024*1024*1024)+1
            d=random.randint(0, 1024)+1
            my_print("INSERT (0, \""+str(k)+"\", "+str(d)+")")
            last[i%20]=k
        if i%50==0:
            my_print("FULLCHECK")
        i+=1


try:
    run_live()
except:
    my_print("FULLCHECK")
    my_print("CLOSE")
