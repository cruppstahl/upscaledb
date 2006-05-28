
all:
	gcc -o btree erase.c buildno.c btree.c find.c insert.c -Wall -g && \
	perl incbuild.pl buildno.c

clean:
	\rm -rf btree *.o

test:
	python regtest/regtest.py
