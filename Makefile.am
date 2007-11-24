                
SUBDIRS = 3rdparty src include samples tools

if HAVE_CPPUNIT
SUBDIRS += unittests
endif

EXTRA_DIST = documentation/Doxyfile                     \
             documentation/tutorial.odt                 \
             CHANGELOG                                  \
             README                                     \
             COPYING.GPL2                               \
             COPYING.GPL3                               \
             3rdparty                                   \
             include/ham/hamsterdb.h                    \
             include/ham/hamsterdb_int.h                \
             include/ham/types.h                        \
             build/scripts/*                            \
             src/*.c src/*.h src/Makefile.am            \
             src/Makefile.in                            \
             unittests/*.h                              \
             unittests/*.hpp                            \
             unittests/data/*                           \
             win32                                      \
             win32/README.TXT                           \
             win32/hamsterdb.sln                        \
             win32/dll.vcproj                           \
             win32/lib.vcproj                           \
             win32/sample_db1.vcproj                    \
             win32/sample_db2.vcproj                    \
             win32/sample_db3.vcproj                    \
             win32/sample_db4.vcproj                    \
             win32/sample_env1.vcproj                   \
             win32/sample_env2.vcproj                   \
             win32/unittests.vcproj                     \
             wince                                      \
             wince/README.TXT                           \
             wince/hamsterdb.sln                        \
             wince/lib.vcproj                           \
             wince/dll.vcproj                           \
             wince/sample_db1.vcproj                    \
             wince/sample_env1.vcproj                   \
             wince/sample_env2.vcproj 

doc:
	doxygen documentation/Doxyfile

test:
	cd unittests && make && ./test
