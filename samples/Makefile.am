
INCLUDES		= -I$(top_builddir)/include

noinst_PROGRAMS	= db1 db2 db3 db4 db5 db6 env1 env2 env3 env4

noinst_BIN      = db1 db2 db3 db4 db5 db6 env1 env2 env3 env4

db1_SOURCES  	= db1.c
db1_LDADD    	= $(top_builddir)/src/libhamsterdb.la

db2_SOURCES  	= db2.c
db2_LDADD    	= $(top_builddir)/src/libhamsterdb.la

db3_SOURCES  	= db3.c
db3_LDADD    	= $(top_builddir)/src/libhamsterdb.la

db4_SOURCES  	= db4.c
db4_LDADD    	= $(top_builddir)/src/libhamsterdb.la

db5_SOURCES  	= db5.c
db5_LDADD    	= $(top_builddir)/src/libhamsterdb.la

db6_SOURCES  	= db6.cpp
db6_LDADD    	= $(top_builddir)/src/libhamsterdb.la

env1_SOURCES  	= env1.c
env1_LDADD    	= $(top_builddir)/src/libhamsterdb.la

env2_SOURCES  	= env2.c
env2_LDADD    	= $(top_builddir)/src/libhamsterdb.la

env3_SOURCES  	= env3.cpp
env3_LDADD    	= $(top_builddir)/src/libhamsterdb.la

env4_SOURCES  	= env4.cpp \
                  calcmodel.hpp \
                  randomgen.hpp \
                  datafeedsim.hpp \
                  mcreporter.hpp \
                  montecarlo.hpp
env4_LDADD    	= $(top_builddir)/src/libhamsterdb.la
