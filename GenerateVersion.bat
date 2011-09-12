@echo off
setlocal

set VERSION=%1
if "%VERSION%" == "" goto usage

msbuild /t:GenerateVersion /p:Version=%VERSION% /p:Configuration=Debug cassandra-sharp.targets
msbuild /t:GenerateVersion /p:Version=%VERSION% /p:Configuration=Release cassandra-sharp.targets

:done
endlocal
goto :eof

:usage
echo %0 ^<version^>
goto :done