
INCLUDES		    = -I$(top_builddir)/include -I$(top_builddir)/src \
					  -I$(top_builddir)/3rdparty/json

hamzilla_SOURCES    = hamzilla.c getopts.c config.c
hamzilla_LDADD      = $(top_builddir)/src/libhamsterdb.la \
                      $(top_builddir)/src/server/libhamserver.la

ham_info_SOURCES    = ham_info.c getopts.c
ham_info_LDADD      = $(top_builddir)/src/libhamsterdb.la

ham_dump_SOURCES    = ham_dump.c getopts.c
ham_dump_LDADD      = $(top_builddir)/src/libhamsterdb.la

ham_recover_SOURCES = ham_recover.c getopts.c
ham_recover_LDADD   = $(top_builddir)/src/libhamsterdb.la

bin_PROGRAMS        = ham_info ham_dump ham_recover
if ENABLE_REMOTE
bin_PROGRAMS        += hamzilla
endif

