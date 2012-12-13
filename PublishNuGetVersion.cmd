@echo off
setlocal

set VERSION=%1
if "%VERSION%" == "" goto usage

ThirdParties\NuGet\NuGet.exe push OutDir\cassandra-sharp.%VERSION%.nupkg

:done
endlocal
goto :eof

:usage
echo %~n0 ^<version^>
goto :done