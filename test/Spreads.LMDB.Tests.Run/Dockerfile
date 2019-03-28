FROM mcr.microsoft.com/dotnet/core-nightly/runtime:3.0.0-preview4-stretch-slim  AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/core-nightly/sdk:3.0.0-preview4-stretch-slim AS build
WORKDIR /src
COPY build/common.props build/
COPY test/Spreads.LMDB.Tests.Run/Spreads.LMDB.Tests.Run.csproj test/Spreads.LMDB.Tests.Run/
COPY test/Spreads.LMDB.Tests/Spreads.LMDB.Tests.csproj test/Spreads.LMDB.Tests/
COPY src/Spreads.LMDB/Spreads.LMDB.csproj src/Spreads.LMDB/
RUN dotnet restore test/Spreads.LMDB.Tests.Run/Spreads.LMDB.Tests.Run.csproj
COPY . .
WORKDIR /src/test/Spreads.LMDB.Tests.Run
RUN dotnet build Spreads.LMDB.Tests.Run.csproj -c Release -o /app

FROM build AS publish
RUN dotnet publish Spreads.LMDB.Tests.Run.csproj -c Release -o /app

FROM base AS final
WORKDIR /app
COPY --from=publish /app .
ENTRYPOINT ["dotnet", "Spreads.LMDB.Tests.Run.dll"]
