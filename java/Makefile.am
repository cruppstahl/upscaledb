if ENABLE_JAVA
SUBDIRS = src java
endif

DIST_SUBDIRS = src java unittests
EXTRA_DIST = README

test:
	cd unittests && $(MAKE) test
