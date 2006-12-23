
SUBDIRS = src include samples 

if HAVE_DB_H
	SUBDIRS += tests
else
	echo 
	echo db.h could not be found; skipping subdirectory ./tests
	echo 
endif

EXTRA_DIST = documentation/Doxyfile 

EXTRA_DIST += include/ham/hamsterdb.h \
          	  include/ham/hamsterdb_int.h \
              include/ham/types.h 

EXTRA_DIST += src/*

EXTRA_DIST += build/scripts/*

doc:
	doxygen documentation/Doxyfile

