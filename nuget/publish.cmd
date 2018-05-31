@for %%f in (..\artifacts\*.nupkg) do nuget push %%f -source https://www.nuget.org/api/v2/package
pause