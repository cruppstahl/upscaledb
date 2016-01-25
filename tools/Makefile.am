
SUBDIRS 			= ups_bench

COMMON				= getopts.h getopts.cc common.h common.c
BOOST_LIBS			= $(BOOST_SYSTEM_LIBS) $(BOOST_THREAD_LIBS)

AM_CPPFLAGS         = -I../include -I$(top_srcdir)/include -I../src \
					  -I../3rdparty/json

upszilla_SOURCES    = upszilla.cc config.h config.c $(COMMON)
upszilla_LDADD      = $(top_builddir)/src/libupscaledb.la \
                      $(top_builddir)/src/5server/libupsserver.la

ups_info_SOURCES    = ups_info.cc $(COMMON)
ups_info_LDADD      = $(top_builddir)/src/libupscaledb.la $(BOOST_LIBS)

ups_dump_SOURCES    = ups_dump.cc $(COMMON)
ups_dump_LDADD      = $(top_builddir)/src/libupscaledb.la

ups_export_SOURCES  = export.pb.cc ups_export.cc $(COMMON)
ups_export_LDADD    = $(top_builddir)/src/.libs/libupscaledb.a \
					  -lprotobuf $(BOOST_LIBS) -ldl
ups_export_LDFLAGS  = $(BOOST_SYSTEM_LDFLAGS) 
if WITH_ZLIB
ups_export_LDADD   += -lz
endif
if WITH_SNAPPY
ups_export_LDADD   += -lsnappy
endif

ups_import_SOURCES  = export.pb.cc ups_import.cc export.pb.h $(COMMON)
ups_import_LDADD    = $(top_builddir)/src/libupscaledb.la -lprotobuf \
					  $(top_builddir)/3rdparty/json/libjson.la

ups_recover_SOURCES = ups_recover.cc $(COMMON)
ups_recover_LDADD   = $(top_builddir)/src/libupscaledb.la

EXTRA_DIST			= upszilla.config export.proto

bin_PROGRAMS        = ups_info ups_dump ups_recover
if ENABLE_REMOTE
bin_PROGRAMS        += upszilla ups_export ups_import
endif

export.pb.cc proto: $(srcdir)/export.proto
	protoc $(srcdir)/export.proto --cpp_out=.

