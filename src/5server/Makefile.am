
AM_CPPFLAGS = -I../../include -I.. -I../../3rdparty

lib_LTLIBRARIES = libupsserver.la

libupsserver_la_SOURCES = upsserver.cc upsserver.h

libupsserver_la_LDFLAGS = -version-info 0:0:0

libupsserver_la_LDFLAGS += $(top_builddir)/3rdparty/json/libjson.la \
                           $(top_builddir)/src/2protobuf/libprotocol.la \
						   $(BOOST_SYSTEM_LIBS) luv -ldl

