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

msbuild /t:GenerateVersion /p:Configuration=%FLAVOR% cassandra-sharp.targets || goto :done

:done
popd
endlocal
goto :eof

:usage
echo usage:
echo    %~n0 ^<flavor^>
echo.
echo where:
echo    flavor  : Debug or Release
goto :done