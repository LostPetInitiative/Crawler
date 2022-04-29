FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY . .
RUN dotnet build -c Release "CrawlerPet911/CrawlerPet911.fsproj" -o /app/build

FROM build AS publish
RUN dotnet publish "CrawlerPet911/CrawlerPet911.fsproj" -c Release -o /app/publish


FROM base AS final
VOLUME /db
WORKDIR /app
COPY --from=publish /app/publish .

CMD dotnet CrawlerPet911.dll -d /db newcards 1000 True
