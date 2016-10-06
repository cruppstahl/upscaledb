
@echo off

copy ..\..\win32\out\java_dll_debug\upscaledb-java.dll .

set CP=.;../java/upscaledb-2.2.1.jar;junit-4.4.jar
echo 1
if ["%JDK%"] == [] goto l1
goto start
:l1

if ["%JDK%"] == [] goto nojdk

:nojdk
echo Neither JDK nor JAVA_HOME is set, exiting
goto end

:error1
echo Compilation failed, exiting
goto end

:start
for %%F in (CursorTest DatabaseTest DatabaseExceptionTest TransactionTest EnvironmentTest) do (
    echo Compiling %%F.java...
    "%JDK%\bin\javac" -cp %CP% %%F.java
    if errorlevel 1 goto error1
    echo Running %%F:
    "%JDK%\bin\java" -cp %CP% org.junit.runner.JUnitCore %%F
    if errorlevel 1 goto error1
)

echo Done! Deleting temporary database files
@del *.db*
goto end

:end
