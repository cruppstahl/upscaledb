ACLOCAL_AMFLAGS	= -I m4


SUBDIRS = 3rdparty src include samples tools unittests

DOCUMENTATION_FILES = documentation/Doxyfile            \
             documentation/hamsterdb-main.jpg           \
             documentation/appendix_gpl2.doc 		    \
             documentation/appendix_gpl3.doc 		    \
             documentation/appendix_gfdl.doc 		    \
             documentation/faq.doc 		                \
             documentation/history.doc 		            \
             documentation/index.doc 		            \
             documentation/samples.doc 		            \
             documentation/starting.doc 		        \
             documentation/Doxyfile
             

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

html-dist: doc
	tar cvzf hamsterdb-html-documentation.$(VERSION).tar.gz documentation/html

test:
	cd unittests && make && ./test

.PHONY: doc test
