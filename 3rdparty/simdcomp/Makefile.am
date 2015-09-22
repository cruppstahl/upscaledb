# If you use Automake then please delete the "makefile". Automake creates
# a "Makefile" (upper-case M), but as long as "makefile" (lower-case M) exists
# then "makefile" will be used and the Automake-"Makefile" is ignored.

# INCLUDES = 
AM_CPPFLAGS = -Iinclude -msse4

noinst_LTLIBRARIES = libsimdcomp.la

libsimdcomp_la_SOURCES = src/simdbitpacking.c \
						 src/simdcomputil.c \
						 src/simdfor.c \
						 src/simdintegratedbitpacking.c \
						 src/simdpackedsearch.c \
						 src/simdpackedselect.c \
						 include/portability.h \
						 include/simdbitpacking.h \
						 include/simdcomp.h \
						 include/simdfor.h \
						 include/simdcomputil.h \
						 include/simdintegratedbitpacking.h

noinst_PROGRAMS = unit
noinst_BIN      = unit

unit_SOURCES    = src/unit.c
unit_LDADD      = libsimdcomp.la

