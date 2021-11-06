FROM mcr.microsoft.com/dotnet/runtime:3.1
COPY CrawlerPet911/bin/Release/netcoreapp3.1/publish /publish
WORKDIR /publish

CMD dotnet CrawlerPet911.dll -d /db newcards 1000 True
