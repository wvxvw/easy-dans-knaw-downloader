#!/bin/sh

set -xe

if [ -z "$(docker network ls -f name=grid -q)" ] ; then
    docker network create grid
fi

docker run \
       -d \
       -p 4442-4444:4442-4444 \
       --net grid \
       --name hub \
       --label role=hub \
       selenium/hub:3.141.59-gold

docker run \
       -d \
       --net grid \
       --name node-0 \
       --label role=worker \
       -e HUB_HOST=hub \
       -e HUB_PORT=4444 \
       -e SE_EVENT_BUS_HOST=hub \
       -e SE_EVENT_BUS_PUBLISH_PORT=4442 \
       -e SE_EVENT_BUS_SUBSCRIBE_PORT=4443 \
       -v /dev/shm:/dev/shm \
       selenium/node-chrome:3.141.59-gold

docker run \
       -d \
       --net grid \
       --name node-1 \
       --label role=worker \
       -e HUB_HOST=hub \
       -e HUB_PORT=4444 \
       -e SE_EVENT_BUS_HOST=hub \
       -e SE_EVENT_BUS_PUBLISH_PORT=4442 \
       -e SE_EVENT_BUS_SUBSCRIBE_PORT=4443 \
       -v /dev/shm:/dev/shm \
       selenium/node-chrome:3.141.59-gold

docker run \
       -d \
       --net grid \
       --name node-2 \
       --label role=worker \
       -e HUB_HOST=hub \
       -e HUB_PORT=4444 \
       -e SE_EVENT_BUS_HOST=hub \
       -e SE_EVENT_BUS_PUBLISH_PORT=4442 \
       -e SE_EVENT_BUS_SUBSCRIBE_PORT=4443 \
       -v /dev/shm:/dev/shm \
       selenium/node-chrome:3.141.59-gold

for c in $(docker ps --filter "label=role=worker" --format "{{.ID}}") ; do
    docker inspect \
           --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
           $c
done
