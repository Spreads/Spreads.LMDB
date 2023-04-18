@echo off

dotnet restore ..\src\Spreads.LMDB
dotnet pack ..\src\Spreads.LMDB -c Release -o \transient\LocalNuget  -p:AutoSuffix=True

pause
