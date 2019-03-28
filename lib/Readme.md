# How to build native libs

Currently native libs are build on Windows, using *Windows Subsystem for Linux* for Linux build.
1. In folder libspreadsdb\src\libspreadsdb change line 30 in Makefile 
to `all: $(SHARED_W)` (Windows) or `all: $(SHARED_L)` (Linux).
2. Run `make` (`mingw32-make` on Windows MinGW or whatever it is 
called on your PATH) in folder libspreadsdb\src\libspreadsdb. 