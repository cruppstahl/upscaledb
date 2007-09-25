                
SUBDIRS = src include samples

if HAVE_CPPUNIT
SUBDIRS += unittests
endif

EXTRA_DIST = documentation/Doxyfile                     \
             documentation/tutorial.odt                 \
             CHANGELOG                                  \
             README                                     \
             COYPING.GPL2                               \
             COYPING.GPL3                               \
             include/ham/hamsterdb.h                    \
             include/ham/hamsterdb_int.h                \
             include/ham/types.h                        \
             build/scripts/*                            \
             src/*.c src/*.h src/Makefile.am            \
             src/Makefile.in                            \
             unittests/memtracker.h                     \
             unittests/data/*                           \
             win32                                      \
             win32/README.TXT                           \
             win32/hamsterdb                            \
             win32/hamsterdb/hamsterdb.sln              \
		     win32/dll/dll.vcproj             			\
		     win32/lib/lib.vcproj             			\
		     win32/sample_db1/sample_db1.vcproj         \
		     win32/sample_db2/sample_db2.vcproj         \
		     win32/sample_db3/sample_db3.vcproj         \
		     win32/sample_db4/sample_db4.vcproj         \
		     win32/unittests/unittests.vcproj		    \
			 wince										\
			 wince/lib									\
			 wince/lib/lib.vcproj						\
			 wince/sample_db1							\
			 wince/sample_db1/sample_db1.vcproj			\
			 wince/sample_env1							\
			 wince/sample_env1/sample_env1.vcproj		\
			 wince/hamsterdb							\
			 wince/hamsterdb/hamsterdb.sln				\
			 wince/README.TXT							\
			 wince/dll									\
			 wince/dll/dll.vcproj

doc:
	doxygen documentation/Doxyfile

test:
	cd unittests && make && ./test
