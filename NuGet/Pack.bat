cd %~dp0

cd ..\ISeriesProvider

call Compile.bat

cd ..\NuGet

del *.nupkg

..\Redist\NuGet Pack linq2db4iSeries.nuspec
