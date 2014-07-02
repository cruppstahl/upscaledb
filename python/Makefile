
.phony: build clean test

all:
	python setup.py build

clean:
	python setup.py clean

test: build
	cd unittests && make
	cd samples && make

