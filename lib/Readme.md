# How to build native libs

Currently native libs are build on Windows, using *Windows Subsystem for Linux* for Linux build.
1. In folder libspreadsdb\src\libspreadsdb change line 30 in Makefile 
to `all: $(SHARED_W)` (Windows) or `all: $(SHARED_L)` (Linux).
2. Run `make` (`mingw32-make` on Windows MinGW or whatever it is 
called on your PATH) in folder libspreadsdb\src\libspreadsdb. 
3. Run compress.bat that copies native shared libs to `lib/out` folder,
 compresses them with raw deflate, and also renames `.so.compressed` 
to just `.compressed` for Linux case because .NET Core has problems with `.so` extension.

Compressed native libraries are packed as resource and then loaded using a 
Bootstrapper utility from Spreads.Core.