FROM mcr.microsoft.com/dotnet/runtime:3.1
COPY publish /publish
WORKDIR /publish

CMD ./CLI new /db
