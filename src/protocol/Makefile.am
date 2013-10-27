
AM_CPPFLAGS     = -DHAM_ENABLE_REMOTE -I$(top_builddir)/include

# INCLUDES =

noinst_LTLIBRARIES     = libprotocol.la

nodist_libprotocol_la_SOURCES = messages.pb.cc
libprotocol_la_SOURCES = protocol.h
libprotocol_la_LIBADD = -lprotobuf

EXTRA_DIST = messages.proto
messages.pb.cc proto: $(srcdir)/messages.proto
	protoc $(srcdir)/messages.proto --cpp_out=.

