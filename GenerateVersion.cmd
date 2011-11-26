@echo off
setlocal

set HERE=%~dp0
pushd %HERE%

set VERSION=%1
if "%VERSION%" == "/?" goto :usage

if "%VERSION%" == "" (
	echo WARNING: missing version as parameter ^(see %~n0 /?^)
	echo WARNING: using VERSION=0.0.0.0 instead
	echo.
	set VERSION=0.0.0.0
)

msbuild /t:GenerateVersion /p:Version=%VERSION% /p:Configuration=Debug cassandra-sharp.targets || goto :nok
msbuild /t:GenerateVersion /p:Version=%VERSION% /p:Configuration=Release cassandra-sharp.targets || goto :nok
msbuild /t:ZipBinaries /p:Version=%VERSION% cassandra-sharp.targets || goto :nok

:done
popd
endlocal
goto :eof

:nok


:usage
echo Usage: %~n0 ^<version^>
echo.
echo where
echo    version : format is AAA.BBB.CCC.DDD
goto :done