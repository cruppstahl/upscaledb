
CP=.:../java/hamsterdb-$(PACKAGE_VERSION).jar:junit-4.4.jar
RUN=LD_LIBRARY_PATH=../../src/.libs/ $(JDK)/bin/java -Djava.library.path=../src/.libs

all: CursorTest.class DatabaseTest.class DatabaseExceptionTest.class \
        EnvironmentTest.class TransactionTest.class


CursorTest.class: CursorTest.java
	$(JDK)/bin/javac -cp $(CP) CursorTest.java

DatabaseTest.class: DatabaseTest.java
	$(JDK)/bin/javac -cp $(CP) DatabaseTest.java

DatabaseExceptionTest.class: DatabaseExceptionTest.java
	$(JDK)/bin/javac -cp $(CP) DatabaseExceptionTest.java

EnvironmentTest.class: EnvironmentTest.java
	$(JDK)/bin/javac -cp $(CP) EnvironmentTest.java

TransactionTest.class: TransactionTest.java
	$(JDK)/bin/javac -cp $(CP) TransactionTest.java

test: all
	$(RUN) -cp $(CP) org.junit.runner.JUnitCore DatabaseTest
	$(RUN) -cp $(CP) org.junit.runner.JUnitCore CursorTest
	$(RUN) -cp $(CP) org.junit.runner.JUnitCore TransactionTest
	$(RUN) -cp $(CP) org.junit.runner.JUnitCore EnvironmentTest
	$(RUN) -cp $(CP) org.junit.runner.JUnitCore DatabaseExceptionTest

clean:
	\rm -rf *.class *.db*
