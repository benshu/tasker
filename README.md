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
    python bash -c " \
        git clone -b improvements-monitor-server-ui https://github.com/wavenator/tasker.git; \
        cd tasker; \
        python setup.py install; \
        python -m tasker.monitor.server \
        --redis-node 127.0.0.1 6379 e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97 0 \
        --redis-node 127.0.0.1 6380 e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97 0 \
        --udp-server 127.0.0.1 9999 \
        --web-server 127.0.0.1 8080 \
    "
```

## Run tests
```shell
python3 -m unittest discover tasker.tests
```
