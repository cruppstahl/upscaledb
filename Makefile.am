ACLOCAL_AMFLAGS	= -I m4


SUBDIRS = 3rdparty src include 
if ENABLE_SERVER
SUBDIRS += server
endif
SUBDIRS += samples tools unittests

DOCUMENTATION_FILES = documentation/Doxyfile*           \
             documentation/hamsterdb-main.jpg           \
             documentation/*.doc	              	    \
             documentation/html
             

EXTRA_DIST = $(DOCUMENTATION_FILES)                     \
             ChangeLog                                  \
             AUTHORS                                    \
             README                                     \
             CREDITS                                    \
             COPYING.GPL2                               \
             COPYING.GPL3                               \
             3rdparty                                   \
             build/scripts/*                            \
             src/*.c src/*.h src/Makefile.am            \
             src/Makefile.in                            \
             tools/*.h                                  \
             unittests/*.h                              \
             unittests/*.hpp                            \
             unittests/*.cpp                            \
             unittests/Makefile.in                      \
             unittests/Makefile.am                      \
             unittests/data/*                           \
             server                                     \
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
             wince/3rdparty_aes.vcproj                 \
             wince/3rdparty_zlib.vcproj                 \
             wince/sample_db1.vcproj                    \
             wince/sample_env1.vcproj                   \
             wince/sample_env2.vcproj 

doc:
	VERSION=$(VERSION) ;            \
	export VERSION;                 \
	doxygen documentation/Doxyfile
	@cp documentation/*.jpg documentation/html/

internal_doc:
	VERSION=$(VERSION) ;            \
	export VERSION;                 \
	doxygen documentation/Doxyfile.int
	@cp documentation/*.jpg documentation/internal/html/

bfc_doc:
	VERSION=$(VERSION) ;            \
	export VERSION;                 \
	doxygen documentation/Doxyfile.bfc

html-dist: doc
	tar cvzf hamsterdb-html-documentation.$(VERSION).tar.gz documentation/html

update_version:
	VERSION=$(VERSION) ;            \
	export VERSION;                 \
	sed 's/hamsterdb-[0-9]*.[0-9]*.[0-9]*.dll/hamsterdb-$(VERSION).dll/' win32/dll.vcproj > win32/_dll.vcproj
	mv win32/_dll.vcproj win32/dll.vcproj
	sed 's/libhamsterdb-[0-9]*.[0-9]*.[0-9]*.lib/libhamsterdb-$(VERSION).lib/' win32/lib.vcproj > win32/_lib.vcproj
	mv win32/_lib.vcproj win32/lib.vcproj

test:
	cd unittests && make && ./test

.PHONY: doc internal_doc bfc_doc html-dist test
