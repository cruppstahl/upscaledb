
AM_CPPFLAGS     = -I../include -I$(top_builddir)/include

noinst_PROGRAMS = db1 db2 db3 db4 db5 db6 env1 env2 env3 uqi1 uqi2

noinst_BIN      = db1 db2 db3 db4 db5 db6 env1 env2 env3 uqi1 uqi2

if ENABLE_REMOTE
noinst_PROGRAMS += server1 client1
noinst_BIN      += server1 client1
endif

LDADD           = $(top_builddir)/src/libupscaledb.la -lstdc++

db1_SOURCES     = db1.c
db1_LDADD       = $(LDADD)

db2_SOURCES     = db2.c
db2_LDADD       = $(LDADD)

db3_SOURCES     = db3.c
db3_LDADD       = $(LDADD)

db4_SOURCES     = db4.c
db4_LDADD       = $(LDADD)

db5_SOURCES     = db5.c
db5_LDADD       = $(LDADD)

db6_SOURCES     = db6.cpp
db6_LDADD       = $(LDADD)

env1_SOURCES    = env1.c
env1_LDADD      = $(LDADD)

env2_SOURCES    = env2.c
env2_LDADD      = $(LDADD)

env3_SOURCES    = env3.cpp
env3_LDADD      = $(LDADD)

server1_SOURCES = server1.c
server1_LDADD   = $(LDADD) \
                  $(top_builddir)/src/5server/libupsserver.la -ldl -lpthread

client1_SOURCES = client1.c
client1_LDADD   = $(LDADD) \
                  $(top_builddir)/src/5server/libupsserver.la -ldl -lpthread

uqi1_SOURCES    = uqi1.c
uqi1_LDADD      = $(LDADD)

uqi2_SOURCES    = uqi2.c
uqi2_LDADD      = $(LDADD)
