
AM_CPPFLAGS     = -I$(top_builddir)/include -I$(JDK_INCLUDE)
if LINUX
AM_CPPFLAGS    += -I$(JDK_INCLUDE)/linux
endif
if DARWIN
AM_CPPFLAGS    += -I$(JDK_INCLUDE)/mac -I$(JDK_INCLUDE)/darwin
endif
AM_CFLAGS       = -Wno-int-to-pointer-cast -Wno-pointer-to-int-cast

lib_LTLIBRARIES = libhamsterdb-java.la

libhamsterdb_java_la_SOURCES = hamsterdb.c

libhamsterdb_java_la_LDFLAGS = -version-info 1:0:0                           \
                               $(top_builddir)/src/libhamsterdb.la

EXTRA_DIST = \
	de_crupp_hamsterdb_CompareCallback.h \
	de_crupp_hamsterdb_Const.h \
	de_crupp_hamsterdb_Cursor.h \
	de_crupp_hamsterdb_Database.h \
	de_crupp_hamsterdb_DatabaseException.h \
	de_crupp_hamsterdb_DuplicateCompareCallback.h \
	de_crupp_hamsterdb_Environment.h \
	de_crupp_hamsterdb_ErrorHandler.h \
	de_crupp_hamsterdb_License.h \
	de_crupp_hamsterdb_Parameter.h \
	de_crupp_hamsterdb_Transaction.h \
	de_crupp_hamsterdb_Version.h

