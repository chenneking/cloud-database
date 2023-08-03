#!/bin/bash

echo "[---------------------BUILDING DOCKER IMAGE---------------------]"
docker build -t kv-client -f kv-client.Dockerfile --platform linux/amd64 .
docker build -t kv-server -f kv-server.Dockerfile --platform linux/amd64 .
docker build -t ecs-server -f ecs-server.Dockerfile --platform linux/amd64 .


echo "[---------------------RENAMING DOCKER IMAGE---------------------]"
docker tag kv-server gitlab.lrz.de:5005/cdb-23/gr1/ms5/kv-server
docker tag kv-client gitlab.lrz.de:5005/cdb-23/gr1/ms5/kv-client
docker tag ecs-server gitlab.lrz.de:5005/cdb-23/gr1/ms5/ecs-server


echo "[---------------------PUSHING DOCKER IMAGE TO REMOTE---------------------]"
docker push gitlab.lrz.de:5005/cdb-23/gr1/ms5/kv-server
docker push gitlab.lrz.de:5005/cdb-23/gr1/ms5/kv-client
docker push gitlab.lrz.de:5005/cdb-23/gr1/ms5/ecs-server