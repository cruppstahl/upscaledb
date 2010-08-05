
INCLUDES		    = -I$(top_builddir)/include -I$(top_builddir)/src

hamsterd_SOURCES    = hamsterd.c getopts.c
hamsterd_LDADD      = $(top_builddir)/src/libhamsterdb.la \
                      $(top_builddir)/server/libhamserver.la

ham_info_SOURCES    = ham_info.c getopts.c
ham_info_LDADD      = $(top_builddir)/src/libhamsterdb.la

ham_dump_SOURCES    = ham_dump.c getopts.c
ham_dump_LDADD      = $(top_builddir)/src/libhamsterdb.la

bin_PROGRAMS        = ham_info ham_dump hamsterd
