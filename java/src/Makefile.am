
AM_CPPFLAGS     = -I$(top_builddir)/include -I$(JDK_INCLUDE)
if LINUX
AM_CPPFLAGS    += -I$(JDK_INCLUDE)/linux
endif
if DARWIN
AM_CPPFLAGS    += -I$(JDK_INCLUDE)/mac -I$(JDK_INCLUDE)/darwin
endif
AM_CFLAGS       = -Wno-int-to-pointer-cast -Wno-pointer-to-int-cast

lib_LTLIBRARIES = libupscaledb-java.la

libupscaledb_java_la_SOURCES = upscaledb.cc

libupscaledb_java_la_LDFLAGS = -version-info 1:0:0                           \
                               $(top_builddir)/src/libupscaledb.la

EXTRA_DIST = \
	de_crupp_upscaledb_CompareCallback.h \
	de_crupp_upscaledb_Const.h \
	de_crupp_upscaledb_Cursor.h \
	de_crupp_upscaledb_Database.h \
	de_crupp_upscaledb_DatabaseException.h \
	de_crupp_upscaledb_Environment.h \
	de_crupp_upscaledb_ErrorHandler.h \
	de_crupp_upscaledb_Parameter.h \
	de_crupp_upscaledb_Transaction.h \
	de_crupp_upscaledb_Result.h \
	de_crupp_upscaledb_Operation.h \
	de_crupp_upscaledb_Version.h

