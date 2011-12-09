
@echo off

set CP=.;../java/hamsterdb-0.0.3.jar;junit-4.4.jar 

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

:start
for %%F in (CursorTest DatabaseTest DatabaseExceptionTest TransactionTest EnvironmentTest) do (
    echo Compiling %%F.java...
    %JDK%\bin\javac -cp %CP% %%F.java
    if errorlevel 1 goto error1
    echo Running %%F:
    %JDK%\bin\java -cp %CP% org.junit.runner.JUnitCore %%F
    if errorlevel 1 goto error1
)

echo Done! Deleting temporary database files (*.db)
@del *.db
goto end

:end
