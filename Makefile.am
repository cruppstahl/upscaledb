ACLOCAL_AMFLAGS = -I m4

SUBDIRS = 3rdparty src include samples tools unittests java

DOCUMENTATION_FILES = documentation/Doxyfile*           \
             documentation/source                       \
             documentation/html


EXTRA_DIST = $(DOCUMENTATION_FILES)                     \
             ChangeLog                                  \
             upscaledb.spec                             \
             AUTHORS                                    \
             README                                     \
             CREDITS                                    \
             COPYING                                    \
             python                                     \
             python/Makefile                            \
             python/MANIFEST                            \
             python/setup.py                            \
             python/samples/db1.py                      \
             python/samples/Makefile                    \
             python/src/python.cc                       \
             python/unittests/cursor.py                 \
             python/unittests/database.py               \
             python/unittests/environment.py            \
             python/unittests/library.py                \
             python/unittests/transaction.py            \
             python/unittests/Makefile                  \
             3rdparty/catch/catch.hpp                   \
             win32/msvc2013/upscaledb.sln               \
             win32/msvc2013/*.vcxproj                   \
             dotnet

doc documentation/html:
	VERSION=$(VERSION) ;            \
	export VERSION;                 \
	doxygen documentation/Doxyfile
 

html-dist: doc
	tar cvzf upscaledb-html-documentation.$(VERSION).tar.gz documentation/html

test:
	cd unittests && make && ./test && ./issue32 -i && ./issue32 -r && ./issue43
if ENABLE_JAVA
	cd java && make test
endif

.PHONY: doc internal_doc bfc_doc html-dist test
