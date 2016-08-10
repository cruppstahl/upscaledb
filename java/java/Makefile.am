
JOPTS=-Xlint:unchecked

EXTRA_DIST = \
     de/crupp/upscaledb/CompareCallback.java \
     de/crupp/upscaledb/Const.java \
     de/crupp/upscaledb/Cursor.java \
     de/crupp/upscaledb/Database.java \
     de/crupp/upscaledb/DatabaseException.java \
     de/crupp/upscaledb/Environment.java \
     de/crupp/upscaledb/ErrorHandler.java \
     de/crupp/upscaledb/Parameter.java \
     de/crupp/upscaledb/Transaction.java \
     de/crupp/upscaledb/Version.java \
     de/crupp/upscaledb/Result.java \
     de/crupp/upscaledb/Operation.java \
     win32.bat

all: de/crupp/upscaledb/Const.class \
     de/crupp/upscaledb/DatabaseException.class \
     de/crupp/upscaledb/Transaction.class \
     de/crupp/upscaledb/Database.class \
     de/crupp/upscaledb/Environment.class \
     de/crupp/upscaledb/Cursor.class \
     de/crupp/upscaledb/Version.class \
     de/crupp/upscaledb/Parameter.class \
     de/crupp/upscaledb/ErrorHandler.class \
     de/crupp/upscaledb/CompareCallback.class \
     de/crupp/upscaledb/Result.class \
     de/crupp/upscaledb/Operation.class \
     jar

jar:
	$(JDK)/bin/jar -cf upscaledb-$(PACKAGE_VERSION).jar \
		de/crupp/upscaledb/*.class 

de/crupp/upscaledb/Const.class: de/crupp/upscaledb/Const.java
	$(JDK)/bin/javac $(JOPTS) de/crupp/upscaledb/Const.java

de/crupp/upscaledb/DatabaseException.class: de/crupp/upscaledb/DatabaseException.java
	$(JDK)/bin/javac $(JOPTS) de/crupp/upscaledb/DatabaseException.java

de/crupp/upscaledb/Database.class: de/crupp/upscaledb/Database.java
	$(JDK)/bin/javac $(JOPTS) de/crupp/upscaledb/Database.java

de/crupp/upscaledb/Environment.class: de/crupp/upscaledb/Environment.java
	$(JDK)/bin/javac $(JOPTS) de/crupp/upscaledb/Environment.java

de/crupp/upscaledb/Cursor.class: de/crupp/upscaledb/Cursor.java
	$(JDK)/bin/javac $(JOPTS) de/crupp/upscaledb/Cursor.java

de/crupp/upscaledb/Version.class: de/crupp/upscaledb/Version.java
	$(JDK)/bin/javac $(JOPTS) de/crupp/upscaledb/Version.java

de/crupp/upscaledb/Result.class: de/crupp/upscaledb/Result.java
	$(JDK)/bin/javac $(JOPTS) de/crupp/upscaledb/Result.java

de/crupp/upscaledb/Parameter.class: de/crupp/upscaledb/Parameter.java
	$(JDK)/bin/javac $(JOPTS) de/crupp/upscaledb/Parameter.java

de/crupp/upscaledb/ErrorHandler.class: de/crupp/upscaledb/ErrorHandler.java
	$(JDK)/bin/javac $(JOPTS) de/crupp/upscaledb/ErrorHandler.java

de/crupp/upscaledb/CompareCallback.class: de/crupp/upscaledb/CompareCallback.java
	$(JDK)/bin/javac $(JOPTS) de/crupp/upscaledb/CompareCallback.java

de/crupp/upscaledb/Transaction.class: de/crupp/upscaledb/Transaction.java
	$(JDK)/bin/javac $(JOPTS) de/crupp/upscaledb/Transaction.java

de/crupp/upscaledb/Operation.class: de/crupp/upscaledb/Operation.java
	$(JDK)/bin/javac $(JOPTS) de/crupp/upscaledb/Operation.java

clean-local:
	rm -rf de/crupp/upscaledb/*.class
	rm -rf *.jar

javah:
	$(JDK)/bin/javah -d ../src de.crupp.upscaledb.CompareCallback
	$(JDK)/bin/javah -d ../src de.crupp.upscaledb.Const
	$(JDK)/bin/javah -d ../src de.crupp.upscaledb.Cursor
	$(JDK)/bin/javah -d ../src de.crupp.upscaledb.DatabaseException
	$(JDK)/bin/javah -d ../src de.crupp.upscaledb.Database
	$(JDK)/bin/javah -d ../src de.crupp.upscaledb.Environment
	$(JDK)/bin/javah -d ../src de.crupp.upscaledb.ErrorHandler
	$(JDK)/bin/javah -d ../src de.crupp.upscaledb.Parameter
	$(JDK)/bin/javah -d ../src de.crupp.upscaledb.Transaction
	$(JDK)/bin/javah -d ../src de.crupp.upscaledb.Version
	$(JDK)/bin/javah -d ../src de.crupp.upscaledb.Result
	$(JDK)/bin/javah -d ../src de.crupp.upscaledb.Operation
