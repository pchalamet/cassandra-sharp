@echo off
setlocal

set VERSION=%1
if "%VERSION%" == "" goto usage

Tools\NuGet\NuGet.exe push OutDir\CassandraSharp.%VERSION%.nupkg

:done
endlocal
goto :eof

:usage
echo %~n0 ^<version^>
goto :done