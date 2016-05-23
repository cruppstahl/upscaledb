# If you use Automake then please delete the "makefile". Automake creates
# a "Makefile" (upper-case M), but as long as "makefile" (lower-case M) exists
# then "makefile" will be used and the Automake-"Makefile" is ignored.

# INCLUDES = 
AM_CPPFLAGS =
if ENABLE_SSE4
AM_CPPFLAGS += -msse4.1
endif

noinst_LTLIBRARIES = libfor.la

libfor_la_SOURCES = for.c for.h

if ENABLE_SSE4
libfor_la_SOURCES += frameofreference.cpp
endif

EXTRA_DIST = for-gen.c


