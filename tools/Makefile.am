
INCLUDES		    = -I$(top_builddir)/include -I$(top_builddir)/src \
					  -I$(top_builddir)/3rdparty/json

hamsrvd_SOURCES     = hamsrvd.c getopts.c config.c
hamsrvd_LDADD       = $(top_builddir)/src/libhamsterdb.la \
                      $(top_builddir)/server/libhamserver.la

ham_info_SOURCES    = ham_info.c getopts.c
ham_info_LDADD      = $(top_builddir)/src/libhamsterdb.la

ham_dump_SOURCES    = ham_dump.c getopts.c
ham_dump_LDADD      = $(top_builddir)/src/libhamsterdb.la

bin_PROGRAMS        = ham_info ham_dump
if ENABLE_REMOTE
bin_PROGRAMS        += hamsrvd
endif
