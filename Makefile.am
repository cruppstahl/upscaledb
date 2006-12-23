
SUBDIRS = src include samples tests

EXTRA_DIST = documentation/Doxyfile 

EXTRA_DIST += include/ham/hamsterdb.h \
          	  include/ham/hamsterdb_int.h \
              include/ham/types.h 

EXTRA_DIST += tests/*.c tests/*.h tests/Makefile.am tests/Makefile.in \
			  tests/README tests/script.tst 

EXTRA_DIST += build/scripts/*

EXTRA_DIST += src/*.c src/*.h src/Makefile.am src/Makefile.in

doc:
	doxygen documentation/Doxyfile

