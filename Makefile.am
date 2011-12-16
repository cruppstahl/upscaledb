ACLOCAL_AMFLAGS	= -I m4


SUBDIRS = 3rdparty src include samples tools unittests java

DOCUMENTATION_FILES = documentation/Doxyfile*           \
             documentation/hamsterdb-main.jpg           \
             documentation/*.doc	              	    \
             documentation/html
             

EXTRA_DIST = $(DOCUMENTATION_FILES)                     \
             ChangeLog                                  \
             hamsterdb.spec                             \
             AUTHORS                                    \
             README                                     \
             CREDITS                                    \
             COPYING.GPL2                               \
             COPYING.GPL3                               \
             3rdparty                                   \
             src/*.cc src/*.h src/Makefile.am   		\
             src/Makefile.in                            \
             src/protocol/messages.proto                \
             src/protocol                               \
             src/server                                 \
             java/*                                     \
             tools/*.h                                  \
             tools/hamzilla.config                      \
             unittests/*.h                              \
             unittests/*.hpp                            \
             unittests/*.cpp                            \
             unittests/Makefile.in                      \
             unittests/Makefile.am                      \
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
             contrib/wince                              \
             contrib/wince/README.TXT                   \
             contrib/wince/hamsterdb.sln                \
             contrib/wince/lib.vcproj                   \
             contrib/wince/dll.vcproj                   \
             contrib/wince/3rdparty_aes.vcproj          \
             contrib/wince/3rdparty_zlib.vcproj         \
             contrib/wince/sample_db1.vcproj            \
             contrib/wince/sample_env1.vcproj           \
             contrib/wince/sample_env2.vcproj 

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

test:
	cd unittests && make && ./test
if ENABLE_JAVA
	cd java && make test
endif

.PHONY: doc internal_doc bfc_doc html-dist test
