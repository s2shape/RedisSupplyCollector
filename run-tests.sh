#!/bin/bash
docker run --name redis -p 6379:6379 -d redis
docker exec -i redis redis-cli < RedisSupplyCollectorTests/tests/emails-utf8.redis

export REDIS_HOST=localhost
export REDIS_PORT=6379

dotnet restore -s https://www.myget.org/F/s2/ -s https://api.nuget.org/v3/index.json
dotnet build
dotnet test
docker stop redis
docker rm redis
