
#
# make sure that our modified CFLAGS are also respected by g++
#
CFLAGS         += -DHAM_ENABLE_REMOTE
AM_CXXFLAGS     = $(CFLAGS)
CXXFLAGS        = $(CFLAGS)

# INCLUDES = 

noinst_LTLIBRARIES     = libprotocol.la

libprotocol_la_SOURCES = protocol.cc messages.pb.cc
libprotocol_la_LDFLAGS = -lprotobuf

messages.pb.cc proto: messages.proto
	protoc messages.proto --cpp_out=.
	

