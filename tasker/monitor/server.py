import os
import asyncio
import functools
import aioredis
import aiohttp
import aiohttp.web
import uvloop
import argparse

from . import statistics
from . import message


class StatisticsUDPServer:
    def __init__(
        self,
        statistics_obj,
    ):
        self.statistics_obj = statistics_obj

    def connection_made(
        self,
        transport,
    ):
        self.transport = transport

    def datagram_received(
        self,
        data,
        addr,
    ):
        message_data = data

        try:
            message_obj = message.Message.unserialize(
                message=message_data,
            )

            self.statistics_obj.process_report(
                message=message_obj,
            )
        except ValueError as exception:
            print(str(exception))

            pass


class StatisticsWebServer:
    def __init__(
        self,
        event_loop,
        host,
        port,
        statistics_obj,
        redis_servers,
    ):
        self.statistics_obj = statistics_obj
        self.statistics_rates = {
            'process_per_second': 0,
            'success_per_second': 0,
            'retry_per_second': 0,
            'failure_per_second': 0,
        }
        self.statistics_metrics = [
            {
                'process': 0,
                'success': 0,
                'retry': 0,
                'failure': 0,
            }
        ] * 5

        self.redis_servers = redis_servers
        self.redis_connections = []

        self.event_loop = event_loop
        self.app = aiohttp.web.Application(
            loop=self.event_loop,
        )

        self.app.router.add_route(
            method='GET',
            path='/ws/statistics',
            handler=self.handle_get_ws_statistics,
        )
        self.app.router.add_route(
            method='GET',
            path='/',
            handler=self.handle_get_root,
        )
        self.app.router.add_static(
            prefix='/dashboard',
            path=os.path.join(
                os.path.dirname(
                    p=os.path.realpath(__file__),
                ),
                'public',
            ),
        )

        web_app_handler = self.app.make_handler(
            loop=self.event_loop,
        )
        self.server = self.event_loop.create_server(
            protocol_factory=web_app_handler,
            host=host,
            port=port,
        )

        self.event_loop.create_task(self.update_rates())

    async def update_rates(
        self,
    ):
        while True:
            await asyncio.sleep(
                delay=1.0,
                loop=self.event_loop,
            )

            self.statistics_metrics = self.statistics_metrics[1:]
            self.statistics_metrics.append(
                {
                    'process': self.statistics_obj.metrics['process'],
                    'success': self.statistics_obj.metrics['success'],
                    'retry': self.statistics_obj.metrics['retry'],
                    'failure': self.statistics_obj.metrics['failure'],
                }
            )

            metrics_differences_sums = {
                'process': [],
                'success': [],
                'retry': [],
                'failure': [],
            }
            for metric_name in metrics_differences_sums:
                for metrics in self.statistics_metrics:
                    for metric_name, metric_value in metrics.items():
                        metrics_differences_sums[metric_name].append(metric_value)

            for metric_name, matric_values in metrics_differences_sums.items():
                metrics_differences = [
                    j - i
                    for i, j in zip(
                        matric_values[:-1],
                        matric_values[1:],
                    )
                ]
                metrics_differences_sums[metric_name] = sum(metrics_differences)

            self.statistics_rates = {
                'process_per_second': metrics_differences_sums['process'] / 5,
                'success_per_second': metrics_differences_sums['success'] / 5,
                'retry_per_second': metrics_differences_sums['retry'] / 5,
                'failure_per_second': metrics_differences_sums['failure'] / 5,
            }

    async def handle_get_ws_statistics(
        self,
        request,
    ):
        websocket_obj = aiohttp.web.WebSocketResponse()

        await websocket_obj.prepare(request)

        async for message in websocket_obj:
            if message.type == aiohttp.WSMsgType.TEXT:
                if message.data == 'metrics':
                    await websocket_obj.send_json(
                        data={
                            'type': 'metrics',
                            'data': {
                                'metrics': self.statistics_obj.metrics,
                                'rates': self.statistics_rates,
                            },
                        },
                    )
                elif message.data == 'queues':
                    queues = {}

                    if not self.redis_connections:
                        for redis_server in self.redis_servers:
                            redis_connection = await aioredis.create_redis(
                                address=(
                                    redis_server['host'],
                                    redis_server['port'],
                                ),
                                password=redis_server['password'],
                                db=redis_server['database'],
                                loop=self.event_loop,
                            )
                            self.redis_connections.append(redis_connection)

                    for redis_connection in self.redis_connections:
                        redis_keys = await redis_connection.keys('*')
                        for key_name in redis_keys:
                            key_name = key_name.decode('utf-8')

                            if key_name.endswith('_lock'):
                                continue
                            elif key_name.endswith('.results'):
                                key_len = await redis_connection.scard(key_name)
                            else:
                                key_len = await redis_connection.llen(key_name)

                            current_key_len = queues.get(
                                key_name,
                                0,
                            )
                            current_key_len += key_len
                            queues[key_name] = current_key_len

                    await websocket_obj.send_json(
                        data={
                            'type': 'queues',
                            'data': queues,
                        },
                    )
                elif message.data == 'workers':
                    workers_dict = self.statistics_obj.workers
                    workers_list = []

                    for hostname in workers_dict:
                        for worker_name in workers_dict[hostname]:
                            worker_metrics = workers_dict[hostname][worker_name]

                            workers_list.append(
                                {
                                    'hostname': hostname,
                                    'name': worker_name,
                                    'metrics': worker_metrics,
                                }
                            )

                    await websocket_obj.send_json(
                        data={
                            'type': 'workers',
                            'data': workers_list,
                        },
                    )

        await websocket_obj.close()

        return websocket_obj

    async def handle_get_root(
        self,
        request,
    ):
        return aiohttp.web.HTTPFound('/dashboard/index.html')


class StatisticsServer:
    def __init__(
        self,
        event_loop,
        web_server,
        udp_server,
        statistics_obj,
        redis_servers,
    ):
        self.statistics_obj = statistics_obj
        self.event_loop = event_loop

        self.statistics_web_server = StatisticsWebServer(
            event_loop=self.event_loop,
            host=web_server['host'],
            port=web_server['port'],
            statistics_obj=statistics_obj,
            redis_servers=redis_servers,
        )

        self.statistics_udp_server = self.event_loop.create_datagram_endpoint(
            protocol_factory=functools.partial(
                StatisticsUDPServer,
                **{
                    'statistics_obj': statistics_obj,
                }
            ),
            local_addr=(
                udp_server['host'],
                udp_server['port'],
            ),
        )

        self.web_server_future = self.event_loop.run_until_complete(self.statistics_web_server.server)
        self.udp_server_future = self.event_loop.run_until_complete(self.statistics_udp_server)

    def close(
        self,
    ):
        self.web_server_future.close()


def main():
    parser = argparse.ArgumentParser(
        description='Tasker Monitor Server',
    )
    parser.add_argument(
        '--redis-node',
        help='host port password database',
        nargs='+',
        type=str,
        required=True,
        action='append',
        dest='redis_nodes',
    )
    parser.add_argument(
        '--web-server',
        nargs=2,
        type=str,
        help='host port',
        required=True,
        dest='web_server',
    )
    parser.add_argument(
        '--udp-server',
        nargs=2,
        type=str,
        help='host port',
        required=True,
        dest='udp_server',
    )

    args = parser.parse_args()

    redis_servers = []
    for redis_server in args.redis_nodes:
        redis_servers.append(
            {
                'host': redis_server[0],
                'port': int(redis_server[1]),
                'password': redis_server[2],
                'database': int(redis_server[3]),
            },
        )

    statistics_obj = statistics.Statistics()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    event_loop = asyncio.new_event_loop()

    StatisticsServer(
        event_loop=event_loop,
        web_server={
            'host': args.web_server[0],
            'port': int(args.web_server[1]),
        },
        udp_server={
            'host': args.udp_server[0],
            'port': int(args.udp_server[1]),
        },
        statistics_obj=statistics_obj,
        redis_servers=redis_servers,
    )

    try:
        event_loop.run_forever()
    except KeyboardInterrupt:
        pass

    event_loop.close()


if __name__ == '__main__':
    main()
