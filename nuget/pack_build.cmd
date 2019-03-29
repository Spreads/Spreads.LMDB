del ..\artifacts\*.nupkg

dotnet restore ..\src\Spreads.LMDB
dotnet pack ..\src\Spreads.LMDB -c Release -o ..\artifacts -p:AutoSuffix=True

pause