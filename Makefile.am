                
SUBDIRS = src include samples 

EXTRA_DIST = documentation/Doxyfile         \
             CHANGELOG                      \
             include/ham/hamsterdb.h        \
             include/ham/hamsterdb_int.h    \
             include/ham/types.h            \
             build/scripts/*                \
             src/*.c src/*.h src/Makefile.am\
             src/Makefile.in                \
             win32                          \
             win32/hamsterdb                \
             win32/hamsterdb/hamsterdb.sln  \
		     win32/hamsterdb/dll/dll.vcproj \
		     win32/hamsterdb/lib/lib.vcproj \
		     win32/hamsterdb/db1/db1.vcproj \
		     win32/hamsterdb/db2/db2.vcproj \
		     win32/hamsterdb/db3/db3.vcproj

doc:
	doxygen documentation/Doxyfile

