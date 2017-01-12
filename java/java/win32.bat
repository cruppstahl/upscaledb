
@echo off

set VERSION=2.2.1

if [%JDK%] == [] goto l1
goto start
:l1
set JDK=%JAVA_HOME%
if [%JDK%] == [] goto nojdk

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
for %%F in (Const DatabaseException Database Environment Cursor Version Parameter ErrorHandler CompareCallback Transaction Operation) do (
    echo Compiling %%F.java...
    %JDK%\bin\javac de/crupp/upscaledb/%%F.java
    if errorlevel 1 goto error1
)

echo Packing JAR file...
%JDK%\bin\jar -cf upscaledb-%VERSION%.jar de/crupp/upscaledb/*.class
if errorlevel 1 goto error2

echo Done!
goto end

:end
