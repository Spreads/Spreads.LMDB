@echo off
dotnet test test/Spreads.LMDB.Tests/Spreads.LMDB.Tests.csproj -c Debug -v n
dotnet test test/Spreads.LMDB.Tests/Spreads.LMDB.Tests.csproj -c Release -v n