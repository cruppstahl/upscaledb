
SUBDIRS 			= ham_bench

COMMON				= getopts.h getopts.c common.h common.c
BOOST_LIBS			= $(BOOST_SYSTEM_LIBS) $(BOOST_THREAD_LIBS)

AM_CPPFLAGS         = -I../include -I$(top_srcdir)/include -I../src \
					  -I../3rdparty/json

hamzilla_SOURCES    = hamzilla.cc config.h config.c $(COMMON)
hamzilla_LDADD      = $(top_builddir)/src/libhamsterdb.la \
                      $(top_builddir)/src/5server/libhamserver.la

ham_info_SOURCES    = ham_info.cc $(COMMON)
ham_info_LDADD      = $(top_builddir)/src/libhamsterdb.la $(BOOST_LIBS)

ham_dump_SOURCES    = ham_dump.cc $(COMMON)
ham_dump_LDADD      = $(top_builddir)/src/libhamsterdb.la

ham_export_SOURCES  = export.pb.cc ham_export.cc $(COMMON)
ham_export_LDADD    = $(top_builddir)/src/.libs/libhamsterdb.a \
					  -lprotobuf $(BOOST_LIBS)
ham_export_LDFLAGS  = $(BOOST_SYSTEM_LDFLAGS) 

ham_import_SOURCES  = export.pb.cc ham_import.cc export.pb.h $(COMMON)
ham_import_LDADD    = $(top_builddir)/src/libhamsterdb.la -lprotobuf \
					  $(top_builddir)/3rdparty/json/libjson.la

ham_recover_SOURCES = ham_recover.cc $(COMMON)
ham_recover_LDADD   = $(top_builddir)/src/libhamsterdb.la

EXTRA_DIST			= hamzilla.config export.proto

bin_PROGRAMS        = ham_info ham_dump ham_recover
if ENABLE_REMOTE
bin_PROGRAMS        += hamzilla ham_export ham_import
endif

export.pb.cc proto: $(srcdir)/export.proto
	protoc $(srcdir)/export.proto --cpp_out=.

