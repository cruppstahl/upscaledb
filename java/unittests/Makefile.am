
CP=.:../java/upscaledb-$(PACKAGE_VERSION).jar:junit-4.4.jar
RUN=$(JDK)/bin/java -Djava.library.path=../src/.libs
LDLP=LD_LIBRARY_PATH=../../src/.libs/

EXTRA_DIST = \
	CursorTest.java \
	DatabaseExceptionTest.java \
	DatabaseTest.java \
	EnvironmentTest.java \
	TransactionTest.java \
	junit-4.4.jar \
	win32.bat

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
	$(LDLP) $(RUN) -cp $(CP) org.junit.runner.JUnitCore DatabaseTest
	$(LDLP) $(RUN) -cp $(CP) org.junit.runner.JUnitCore CursorTest
	$(LDLP) $(RUN) -cp $(CP) org.junit.runner.JUnitCore TransactionTest
	$(LDLP) $(RUN) -cp $(CP) org.junit.runner.JUnitCore DatabaseExceptionTest
	$(LDLP) $(RUN) -cp $(CP) org.junit.runner.JUnitCore EnvironmentTest

clean-local:
	rm -rf *.class *.db*
