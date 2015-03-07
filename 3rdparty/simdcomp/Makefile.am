# If you use Automake then please delete the "makefile". Automake creates
# a "Makefile" (upper-case M), but as long as "makefile" (lower-case M) exists
# then "makefile" will be used and the Automake-"Makefile" is ignored.

# INCLUDES = 
AM_CPPFLAGS = -Iinclude -mavx

noinst_LTLIBRARIES = libsimdcomp.la

libsimdcomp_la_SOURCES = src/simdbitpacking.c \
						 src/simdcomputil.c \
						 src/simdintegratedbitpacking.c \
						 src/varintdecode.c \
						 include/simdbitpacking.h \
						 include/simdcomp.h \
						 include/simdcomputil.h \
						 include/simdintegratedbitpacking.h

