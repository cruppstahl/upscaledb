
bin_PROGRAMS	= ups_bench
AM_CPPFLAGS     = -I$(top_builddir)/include -I.. -I$(top_builddir)/src 

if WITH_BERKELEYDB
AM_CPPFLAGS     += -DUPS_WITH_BERKELEYDB
endif

ups_bench_SOURCES = berkeleydb.h \
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
				  upscaledb.h \
				  upscaledb.cc \
				  main.cc \
				  metrics.h \
				  misc.h \
				  mutex.h \
				  timer.h

ups_bench_LDADD = $(top_builddir)/src/.libs/libupscaledb.a \
				  $(BOOST_SYSTEM_LIBS) \
				  $(BOOST_THREAD_LIBS) $(BOOST_FILESYSTEM_LIBS) \
				  -lpthread -ldl
ups_bench_LDFLAGS =

if DARWIN
ups_bench_LDADD   += $(BOOST_SYSTEM_LIBS) -lboost_chrono-mt
ups_bench_LDFLAGS += $(BOOST_SYSTEM_LDFLAGS)
else
ups_bench_LDADD   += $(BOOST_CHRONO_LIBS)
ups_bench_LDFLAGS += $(BOOST_CHRONO_LDFLAGS)
endif

if ENABLE_REMOTE
AM_CPPFLAGS     += -DUPS_ENABLE_REMOTE
ups_bench_LDADD += $(top_builddir)/src/5server/.libs/libupsserver.a \
				   -lprotobuf -luv -ldl
endif

if ENABLE_COMPRESSION
if WITH_ZLIB
ups_bench_LDADD += -lz
endif
if WITH_SNAPPY
ups_bench_LDADD += -lsnappy
endif
if WITH_LZOP
ups_bench_LDADD += -llzo2
endif
endif

if ENABLE_COMPRESSION
ups_bench_LDADD += -lcrypto
endif
