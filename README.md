# tasker


### Start a redis server
```shell
docker run \
    --interactive \
    --tty \
    --rm \
    --net=host \
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
    "
```

### Start a monitoring server
```shell
docker run \
    --interactive \
    --tty \
    --rm \
    --log-driver=json-file --log-opt=max-size=10m \
    --publish=9999:9999/udp \
    --publish=8080:8080 \
    python bash -c " \
        pip3 install git+https://github.com/wavenator/tasker.git; \
        python -m tasker.monitor.server; \
    "
```
