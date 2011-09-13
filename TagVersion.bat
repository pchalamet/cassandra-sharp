@echo off
setlocal

set VERSION=%1
if "%VERSION%" == "" goto usage

svn copy https://cassandra-sharp.googlecode.com/svn/trunk https://cassandra-sharp.googlecode.com/svn/tags/%VERSION% -m "Release %VERSION%"

:done
endlocal
goto :eof

:usage
echo %0 ^<version^>
goto :done