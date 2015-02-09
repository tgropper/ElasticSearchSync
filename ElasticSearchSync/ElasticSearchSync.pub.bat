:: nuget spec
set /p version=Version number:
nuget pack ElasticSearchSync.csproj -Prop Configuration=Debug -Symbols -IncludeReferencedProjects -Version %version%
nuget push ElasticSearchSync.%version%.nupkg
pause;