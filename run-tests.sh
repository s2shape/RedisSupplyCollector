#!/bin/bash
docker run --name redis -p 6379:6379 -d redis
docker exec -i redis redis-cli < RedisSupplyCollectorTests/tests/emails-utf8.redis

echo { > RedisSupplyCollectorTests/Properties/launchSettings.json
echo   \"profiles\": { >> RedisSupplyCollectorTests/Properties/launchSettings.json
echo     \"RedisSupplyCollectorTests\": { >> RedisSupplyCollectorTests/Properties/launchSettings.json
echo       \"commandName\": \"Project\", >> RedisSupplyCollectorTests/Properties/launchSettings.json
echo       \"environmentVariables\": { >> RedisSupplyCollectorTests/Properties/launchSettings.json
echo         \"REDIS_HOST\": \"localhost\", >> RedisSupplyCollectorTests/Properties/launchSettings.json
echo         \"REDIS_PORT\": \"6379\" >> RedisSupplyCollectorTests/Properties/launchSettings.json
echo       } >> RedisSupplyCollectorTests/Properties/launchSettings.json
echo     } >> RedisSupplyCollectorTests/Properties/launchSettings.json
echo   } >> RedisSupplyCollectorTests/Properties/launchSettings.json
echo } >> RedisSupplyCollectorTests/Properties/launchSettings.json

dotnet build
dotnet test
docker stop redis
docker rm redis
