@echo off

@if "%1"=="x86" goto set_x86
@if "%1"=="x64" goto set_x64
@if "%1"=="" goto error

:set_x86
@echo Setting environment for using Microsoft Visual Studio 2010 x86 tools.

set INCLUDE=^
c:\Program Files\Microsoft Visual Studio 10.0\VC\include;^
c:\Program Files\Microsoft SDKs\Windows\v7.0A\include;

set LIB=^
c:\Program Files\Microsoft Visual Studio 10.0\VC\lib;^
c:\Program Files\Microsoft SDKs\Windows\v7.0A\lib;

set PATH=^
%SystemRoot%\system32;^
c:\Program Files\Microsoft Visual Studio 10.0\VC\bin;^
c:\program files\microsoft visual studio 10.0\common7\ide;

goto test_bin_locations

:set_x64
@echo Setting environment for using Microsoft Visual Studio 2015 x64 tools.

set INCLUDE=^
C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\INCLUDE;^
C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\ATLMFC\INCLUDE;^
C:\Program Files (x86)\Windows Kits\10\include\10.0.10586.0\ucrt;^
C:\Program Files (x86)\Windows Kits\10\include\10.0.10586.0\shared;^
C:\Program Files (x86)\Windows Kits\10\include\10.0.10586.0\um;^
C:\Program Files (x86)\Windows Kits\10\include\10.0.10586.0\winrt;

set LIB=^
C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\LIB\amd64;^
C:\Program Files (x86)\Windows Kits\10\lib\10.0.10586.0\ucrt\x64;^
C:\Program Files (x86)\Windows Kits\10\lib\10.0.10586.0\um\x64;

set PATH=^
C:\Program Files (x86)\MSBuild\14.0\bin\amd64;^
C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\BIN\amd64;^
C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\VCPackages;^
C:\Program Files (x86)\Microsoft Visual Studio 14.0\Common7\IDE;^
C:\Program Files (x86)\Microsoft Visual Studio 14.0\Common7\Tools;^
C:\Program Files (x86)\Windows Kits\10\bin\x64;^
C:\Program Files (x86)\Windows Kits\10\bin\x86;^
C:\WINDOWS\system32;

goto test_bin_locations

:test_bin_locations
@echo on
where nmake
where cl.exe
where link.exe
@echo off
goto:eof

:error
@echo Usage: setenv.bat [x86^|x64]

goto:eof