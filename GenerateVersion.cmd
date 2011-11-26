@echo off
setlocal

set HERE=%~dp0
pushd %HERE%

set FLAVOR=%1
if "%FLAVOR%" == "/?" goto :usage
if "%FLAVOR%" == "" (
	echo WARNING: missing flavor as parameter ^(see %~n0 /?^)
	echo WARNING: using FLAVOR=Debug instead
	echo.
	set FLAVOR=Debug
)

set VERSION=%2
if "%VERSION%" == "" (
	echo WARNING: missing version as parameter ^(see %~n0 /?^)
	echo WARNING: using VERSION=0.0.0.0 instead
	echo.
	set VERSION=0.0.0.0
)

msbuild /t:GenerateVersion /p:Version=%VERSION% /p:Configuration=%FLAVOR% cassandra-sharp.targets || goto :done

:done
popd
endlocal
goto :eof

:usage
echo usage:
echo    %~n0 ^<flavor^> ^<version^>
echo.
echo where:
echo    flavor  : Debug or Release
echo    version : format is AAA.BBB.CCC.DDD
goto :done