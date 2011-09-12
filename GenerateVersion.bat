@echo off
setlocal

set VERSION=%1
if "%VERSION%" == "" goto usage

msbuild /t:GenerateVersion /p:Version=%VERSION% /p:Configuration=Debug cassandra-sharp.targets || exit /b 5
msbuild /t:GenerateVersion /p:Version=%VERSION% /p:Configuration=Release cassandra-sharp.targets || exit /b 5
msbuild /t:ZipBinaries /p:Version=%VERSION% cassandra-sharp.targets || exit /b 5

:done
endlocal
goto :eof

:usage
echo %0 ^<version^>
goto :done