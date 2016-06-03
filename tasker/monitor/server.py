import asyncio
import os
import jinja2
import functools
import aiohttp
import aiohttp.web

from . import statistics
from . import host
from . import worker
from . import message


class StatisticsUDPServer:
    '''
    '''
    def __init__(self, statistics_obj):
        self.statistics_obj = statistics_obj

    def connection_made(self, transport):
        '''
        '''
        self.transport = transport

    def datagram_received(self, data, addr):
        '''
        '''
        message_data = data

        try:
            message_obj = message.Message.unserialize(message_data)

            self.dispatch_message(message_obj)
        except ValueError as exc:
            print(str(exc))
            pass

    def dispatch_message(self, message_obj):
        host_obj = host.Host(
            name=message_obj.hostname,
        )

        worker_obj = worker.Worker(
            name=message_obj.worker_name,
        )

        self.statistics_obj.ensure_host(
            host=host_obj,
        )

        self.statistics_obj.ensure_worker(
            host=host_obj,
            worker=worker_obj,
        )

        self.statistics_obj.report_worker(
            host=host_obj,
            worker=worker_obj,
            message=message_obj,
        )


class StatisticsWebServer:
    '''
    '''
    def __init__(self, event_loop, host, port, statistics_obj):
        self.statistics_obj = statistics_obj
        self.event_loop = event_loop
        self.app = aiohttp.web.Application(
            loop=self.event_loop,
        )

        self.app.router.add_route(
            method='GET',
            path='/',
            handler=self.handle_get_main,
        )
        self.app.router.add_route(
            method='GET',
            path='/statistics',
            handler=self.handle_get_statistics,
        )

        self.server = self.event_loop.create_server(
            protocol_factory=self.app.make_handler(),
            host=host,
            port=port,
        )

    @asyncio.coroutine
    def handle_get_main(self, request):
        '''
        '''
        statistics = self.statistics_obj.statistics
        templates_dir = '{current_path}/interface/templates'.format(
            current_path=os.path.dirname(os.path.realpath(__file__)),
        )

        dashboard_template_html = ''
        with open(os.path.join(templates_dir, 'dashboard.tpl')) as dashboard_template_file:
            dashboard_template_html = dashboard_template_file.read()

        template = jinja2.Template(dashboard_template_html)
        html = template.render(
            statistics=statistics,
        )

        return aiohttp.web.Response(
            body=html.encode('utf-8'),
        )

    @asyncio.coroutine
    def handle_get_statistics(self, request):
        '''
        '''
        statistics = self.statistics_obj.statistics

        return aiohttp.web.json_response(
            data=statistics,
        )


class StatisticsServer:
    '''
    '''
    def __init__(self, event_loop, web_server, udp_server, statistics_obj):
        self.statistics_obj = statistics_obj
        self.event_loop = event_loop

        self.statistics_web_server = StatisticsWebServer(
            event_loop=self.event_loop,
            host=web_server['host'],
            port=web_server['port'],
            statistics_obj=statistics_obj,
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

    def close(self):
        self.web_server_future.close()


def main():
    statistics_obj = statistics.Statistics()
    event_loop = asyncio.new_event_loop()

    StatisticsServer(
        event_loop=event_loop,
        web_server={
            'host': '0.0.0.0',
            'port': 8080,
        },
        udp_server={
            'host': '0.0.0.0',
            'port': 9999,
        },
        statistics_obj=statistics_obj,
    )

    try:
        event_loop.run_forever()
    except KeyboardInterrupt:
        pass

    event_loop.close()


if __name__ == '__main__':
    main()
