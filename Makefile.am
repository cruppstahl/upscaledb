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

update_version:
	VERSION=$(VERSION)
	OLD_VERSION=$(OLD_VERSION)
	sed 's/hamsterdb-$(OLD_VERSION).dll/hamsterdb-$(VERSION).dll/' win32/dll.vcproj > win32/_dll.vcproj
	mv win32/_dll.vcproj win32/dll.vcproj
	sed 's/libhamsterdb-$(OLD_VERSION).lib/libhamsterdb-$(VERSION).lib/' win32/lib.vcproj > win32/_lib.vcproj
	mv win32/_lib.vcproj win32/lib.vcproj
	sed 's/hamserver-$(OLD_VERSION).dll/hamserver-$(VERSION).dll/' win32/server_dll.vcproj > win32/_server_dll.vcproj
	mv win32/_server_dll.vcproj win32/server_dll.vcproj
	sed 's/libhamserver-$(OLD_VERSION).lib/libhamserver-$(VERSION).lib/' win32/server_lib.vcproj > win32/_server_lib.vcproj
	mv win32/_server_lib.vcproj win32/server_lib.vcproj
	sed 's/Version: $(OLD_VERSION)/Version: $(VERSION)/' hamsterdb.spec > _hamsterdb.spec
	mv _hamsterdb.spec hamsterdb.spec
	sed 's/@version $(OLD_VERSION)/@version $(VERSION)/' include/ham/hamsterdb.h > /tmp/hamsterdb.h
	mv /tmp/hamsterdb.h include/ham/hamsterdb.h
	sed 's/@version $(OLD_VERSION)/@version $(VERSION)/' include/ham/hamsterdb.hpp > /tmp/hamsterdb.hpp
	mv /tmp/hamsterdb.hpp include/ham/hamsterdb.hpp
	sed 's/$(OLD_VERSION)/$(VERSION)/' README > /tmp/README
	mv /tmp/README README
	sed 's/$(OLD_VERSION)/$(VERSION)/' src/version.h > /tmp/version.h
	mv /tmp/version.h src/version.h
	sed 's/$(OLD_VERSION)/$(VERSION)/' documentation/Doxyfile > /tmp/Doxyfile
	mv /tmp/Doxyfile documentation/Doxyfile
	sed 's/$(OLD_VERSION)/$(VERSION)/' documentation/Doxyfile.bfc > /tmp/Doxyfile
	mv /tmp/Doxyfile documentation/Doxyfile.bfc
	sed 's/$(OLD_VERSION)/$(VERSION)/' documentation/Doxyfile.int > /tmp/Doxyfile
	mv /tmp/Doxyfile documentation/Doxyfile.int
	@echo
	@echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	@echo Manually update src/version.h
	@echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

test:
	cd unittests && make && ./test
if ENABLE_JAVA
	cd ../java && make test
endif

.PHONY: doc internal_doc bfc_doc html-dist test
