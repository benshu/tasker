# tasker


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
        git clone -b improved_version https://github.com/wavenator/tasker.git; \
        cd tasker/tasker/monitor/server; \
        npm install; \
        node server.js \
            --redis_host=127.0.0.1 \
            --redis_port=6379 \
            --redis_password=e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97 \
            --udp_server_bind_port=9999 \
            --udp_server_bind_host=0.0.0.0 \
            --web_server_bind_port=8080 \
            --web_server_bind_host=0.0.0.0 \
    "
```
