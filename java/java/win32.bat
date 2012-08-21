
@echo off

set VER_MAJ=2
set VER_MIN=0
set VER_REV=0.rc3

if ["%JDK%"] == [] goto l1
goto start
:l1
set JDK=%JAVA_HOME%
if ["%JDK%"] == [] goto nojdk

:nojdk
echo Neither JDK nor JAVA_HOME is set, exiting
goto end

:error1
echo Compilation failed, exiting
goto end

:error2
echo Creating JAR archive failed, exiting
goto end

:start
for %%F in (Const DatabaseException Database Environment Cursor Version License Parameter ErrorHandler CompareCallback PrefixCompareCallback DuplicateCompareCallback Transaction) do (
	echo Compiling %%F.java...
	"%JDK%\bin\javac" de/crupp/hamsterdb/%%F.java
	if errorlevel 1 goto error1
)

echo Packing JAR file...
"%JDK%\bin\jar" -cf hamsterdb-%VER_MAJ%.%VER_MIN%.%VER_REV%.jar de/crupp/hamsterdb/*.class
if errorlevel 1 goto error2

echo Done!
goto end

:end
