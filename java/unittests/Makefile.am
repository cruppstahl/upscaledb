
CP=.:../java/hamsterdb-$(PACKAGE_VERSION).jar:junit-4.4.jar 

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

run test: all
	$(JDK)/bin/java -cp $(CP) org.junit.runner.JUnitCore TransactionTest
	$(JDK)/bin/java -cp $(CP) org.junit.runner.JUnitCore EnvironmentTest
	$(JDK)/bin/java -cp $(CP) org.junit.runner.JUnitCore DatabaseTest
	$(JDK)/bin/java -cp $(CP) org.junit.runner.JUnitCore DatabaseExceptionTest
	$(JDK)/bin/java -cp $(CP) org.junit.runner.JUnitCore CursorTest

clean:
	\rm *.class *.db
