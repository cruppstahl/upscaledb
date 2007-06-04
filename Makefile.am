                
SUBDIRS = src include samples

if HAVE_CPPUNIT
SUBDIRS += unittests
endif 

EXTRA_DIST = documentation/Doxyfile                     \
             documentation/tutorial.odt                 \
             CHANGELOG                                  \
             include/ham/hamsterdb.h                    \
             include/ham/hamsterdb_int.h                \
             include/ham/types.h                        \
             build/scripts/*                            \
             src/*.c src/*.h src/Makefile.am            \
             src/Makefile.in                            \
             unittests/memtracker.h                     \
             win32                                      \
             win32/README.TXT                           \
             win32/hamsterdb                            \
             win32/hamsterdb/hamsterdb.sln              \
		     win32/dll/dll.vcproj             			\
		     win32/lib/lib.vcproj             			\
		     win32/sample_db1/sample_db1.vcproj         \
		     win32/sample_db2/sample_db2.vcproj         \
		     win32/sample_db3/sample_db3.vcproj         \
		     win32/unittests/unittests.vcproj

doc:
	doxygen documentation/Doxyfile

test:
	cd unittests && make && ./test
