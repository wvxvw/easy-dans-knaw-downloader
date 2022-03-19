#!/bin/sh

# Run with: sudo -E env PATH=$PATH ./test.sh

set -ex

NODES=

for c in $(docker ps --filter "label=role=worker" --format "{{.ID}}") ; do
    IP=$(docker inspect \
                --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
                $c)
    NODES="--node $IP $NODES"
done

python \
    -m easy_dans_knaw_downloader \
    --output ./out \
    --dataset 112935 \
    --verbosity 20 \
    $NODES
