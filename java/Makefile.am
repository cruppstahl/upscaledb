if ENABLE_JAVA
SUBDIRS = src java
endif

EXTRA_DIST = src java unittests

test:
	cd unittests && make test

clean:
	cd unittests && make clean
	cd java && make clean
	cd src && make clean
