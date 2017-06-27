# tasker

[![Build Status](https://travis-ci.org/wavenator/tasker.svg?branch=master)](https://travis-ci.org/wavenator/tasker)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/eae56b505e034d9785d6bce47ed04355)](https://www.codacy.com/app/wavenator/tasker?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=wavenator/tasker&amp;utm_campaign=Badge_Grade)
### Start redis servers
```shell
docker run \
    --interactive \
    --tty \
    --rm \
    --publish=6379:6379 \
    --privileged \
    --log-driver=json-file --log-opt=max-size=10m \
    redis bash -c " \
        echo 65535 > /proc/sys/net/core/somaxconn; \
        echo never > /sys/kernel/mm/transparent_hugepage/enabled; \
        echo 1 > /proc/sys/vm/overcommit_memory; \
        redis-server \
            --maxclients 65535 \
            --save '' \
            --tcp-backlog 65535 \
            --tcp-keepalive 10 \
            --maxmemory 1gb \
            --maxmemory-policy noeviction \
            --protected-mode no \
            --bind 0.0.0.0 \
            --requirepass e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97 \
    "
docker run \
    --interactive \
    --tty \
    --rm \
    --publish=6380:6379 \
    --privileged \
    --log-driver=json-file --log-opt=max-size=10m \
    redis bash -c " \
        echo 65535 > /proc/sys/net/core/somaxconn; \
        echo never > /sys/kernel/mm/transparent_hugepage/enabled; \
        echo 1 > /proc/sys/vm/overcommit_memory; \
        redis-server \
            --maxclients 65535 \
            --save '' \
            --tcp-backlog 65535 \
            --tcp-keepalive 10 \
            --maxmemory 1gb \
            --maxmemory-policy noeviction \
            --protected-mode no \
            --bind 0.0.0.0 \
            --requirepass e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97 \
    "
```

### Start a monitoring server
```shell
docker run \
    --interactive \
    --tty \
    --rm \
    --net=host \
    --log-driver=json-file --log-opt=max-size=10m \
    --publish=9999:9999/udp \
    --publish=8080:8080 \
    node bash -c " \
        git clone -b master https://github.com/wavenator/tasker.git; \
        cd tasker/tasker/monitor/server; \
        npm install; \
        node server.js \
            --redis_node=redis://:e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97@127.0.0.1:6379/0 \
            --redis_node=redis://:e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97@127.0.0.1:6380/0 \
            --udp_server_bind_port=9999 \
            --udp_server_bind_host=0.0.0.0 \
            --web_server_bind_port=8080 \
            --web_server_bind_host=0.0.0.0 \
    "
```

## Run tests
```shell
python3 -m unittest discover tasker.tests
```
