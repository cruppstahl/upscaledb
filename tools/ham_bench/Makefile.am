
bin_PROGRAMS	= ham_bench
AM_CPPFLAGS     = -I$(top_builddir)/include -I..

if WITH_BERKELEYDB
AM_CPPFLAGS     += -DHAM_WITH_BERKELEYDB
endif

ham_bench_SOURCES = berkeleydb.h \
				  berkeleydb.cc \
				  configuration.h \
				  database.h \
				  database.cc \
				  datasource.h \
				  datasource_binary.h \
				  datasource_numeric.h \
				  datasource_string.h \
				  generator.h \
				  generator_parser.h \
				  generator_parser.cc \
				  generator_runtime.h \
				  generator_runtime.cc \
				  ../getopts.h \
				  ../getopts.c \
				  ../common.h \
				  ../common.c \
				  graph.h \
				  hamsterdb.h \
				  hamsterdb.cc \
				  main.cc \
				  metrics.h \
				  misc.h \
				  mutex.h \
				  timer.h

ham_bench_LDADD = $(top_builddir)/src/.libs/libhamsterdb.a \
				  $(BOOST_SYSTEM_LIBS) \
				  $(BOOST_THREAD_LIBS) $(BOOST_FILESYSTEM_LIBS) \
				  -lpthread -ldl
ham_bench_LDFLAGS =

if DARWIN
ham_bench_LDADD   += $(BOOST_SYSTEM_LIBS) -lboost_chrono-mt
ham_bench_LDFLAGS += $(BOOST_SYSTEM_LDFLAGS)
else
ham_bench_LDADD   += $(BOOST_CHRONO_LIBS)
ham_bench_LDFLAGS += $(BOOST_CHRONO_LDFLAGS)
endif

if ENABLE_REMOTE
AM_CPPFLAGS     += -DHAM_ENABLE_REMOTE
ham_bench_LDADD += $(top_builddir)/src/server/.libs/libhamserver.a \
				   -lprotobuf -luv -ldl
endif

