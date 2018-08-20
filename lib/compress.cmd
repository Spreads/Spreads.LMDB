xcopy /Y "%~dp0libspreadsdb\src\libspreadsdb\libspreads_lmdb.dll" "%~dp0out\w64\bin"
xcopy /Y "%~dp0libspreadsdb\src\libspreadsdb\libspreads_lmdb.so" "%~dp0out\l64\bin"

%~dp0/../tools/Bootstrapper.exe -p *spreads_lmdb.dll  ./out
%~dp0/../tools/Bootstrapper.exe -p *spreads_lmdb.so  ./out

move /Y "%~dp0out\l64\bin\libspreads_lmdb.so.compressed" "%~dp0out\l64\bin\libspreads_lmdb.compressed"

pause