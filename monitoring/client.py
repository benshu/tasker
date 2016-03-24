import datetime
import requests
import enum
import elasticsearch


class MessageType(enum.Enum):
    install = 0
    success = 1
    failure = 2
    retry = 3


class MonitoringMessage:
    def __init__(self, host_name, worker_name, message_type, date):
        self.host_name = host_name
        self.worker_name = worker_name
        self.message_type = message_type
        self.date = date

    @property
    def encoded(self):
        message = {
            'host_name': self.host_name,
            'worker_name': self.worker_name,
            'type': self.message_type.value,
            'date': self.date,
        }

        return message


class MonitoringClient:
    '''
    '''
    index_name = 'monitoring'

    def __init__(self, monitoring_server, host_name, worker_name):
        self.host_name = host_name
        self.worker_name = worker_name
        self.monitoring_server = monitoring_server

        self.elasticsearch_instance = elasticsearch.Elasticsearch(
            hosts=[
                {
                    'host': self.monitoring_server['host'],
                    'port': self.monitoring_server['port'],
                }
            ],
        )

    def _send_stats(self, message_type):
        message = MonitoringMessage(
            host_name=self.host_name,
            worker_name=self.worker_name,
            message_type=MessageType[message_type],
            date=datetime.datetime.utcnow(),
        )

        index_result = self.elasticsearch_instance.index(
            index=self.index_name,
            doc_type='metric',
            body=message.encoded,
        )

        return index_result['created']

    def send_success(self):
        self._send_stats(
            message_type='success',
        )

    def send_failure(self):
        self._send_stats(
            message_type='failure',
        )

    def send_retry(self):
        self._send_stats(
            message_type='retry',
        )

    def send_install(self):
        self._send_stats(
            message_type='install',
        )

    def install_kibana(self, kibana_host, kibana_port):
        kibana_version = self._get_kibana_version()

        self._install_kibana_default_index(
            kibana_host=kibana_host,
            kibana_port=kibana_port,
            kibana_version=kibana_version,
        )

        self._install_kibana_visualizations(
            kibana_host=kibana_host,
            kibana_port=kibana_port,
            kibana_version=kibana_version,
        )

        self._install_kibana_dashboard(
            kibana_host=kibana_host,
            kibana_port=kibana_port,
            kibana_version=kibana_version,
        )

    def _get_kibana_version(self):
        results = self.elasticsearch_instance.search(
            q='_type:"config"',
            index='.kibana',
        )

        if not results:
            raise Exception('could not measure kibana\'s version')

        return results['hits']['hits'][0]['_id']

    def _install_kibana_default_index(self, kibana_host, kibana_port, kibana_version):
        requests.post(
            url='http://{kibana_host}:{kibana_port}/elasticsearch/.kibana/index-pattern/{index_name}?op_type=create'.format(
                kibana_host=kibana_host,
                kibana_port=kibana_port,
                index_name=self.index_name,
            ),
            json={
                'title': 'monitoring',
                'timeFieldName': 'date',
            },
            headers={
                'kbn-version': kibana_version,
            },
        )

        requests.post(
            url='http://{kibana_host}:{kibana_port}/elasticsearch/.kibana/config/{kibana_version}/_update'.format(
                kibana_host=kibana_host,
                kibana_port=kibana_port,
                kibana_version=kibana_version,
            ),
            json={
                'doc': {
                    'defaultIndex': self.index_name,
                }
            },
            headers={
                'kbn-version': kibana_version,
            },
        )

    def _install_kibana_visualizations(self, kibana_host, kibana_port, kibana_version):
        visualizations = {
            'total_success_counter': {
                "title": "total_success_counter",
                "visState": "{\"title\":\"total_success_counter\",\"type\":\"metric\",\"params\":{\"fontSize\":60},\"aggs\":[{\"id\":\"1\",\"type\":\"count\",\"schema\":\"metric\",\"params\":{}}],\"listeners\":{}}",
                "uiStateJSON": "{}",
                "description": "",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"index\":\"monitoring\",\"query\":{\"query_string\":{\"query\":\"type:1\",\"analyze_wildcard\":true}},\"filter\":[]}"
                }
            },
            'total_retry_counter': {
                "title": "total_retry_counter",
                "visState": "{\"title\":\"total_retry_counter\",\"type\":\"metric\",\"params\":{\"fontSize\":60},\"aggs\":[{\"id\":\"1\",\"type\":\"count\",\"schema\":\"metric\",\"params\":{}}],\"listeners\":{}}",
                "uiStateJSON": "{}",
                "description": "",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"index\":\"monitoring\",\"query\":{\"query_string\":{\"query\":\"type:3\",\"analyze_wildcard\":true}},\"filter\":[]}"
                }
            },
            'total_failure_counter': {
                "title": "total_failure_counter",
                "visState": "{\"title\":\"total_failure_counter\",\"type\":\"metric\",\"params\":{\"fontSize\":60},\"aggs\":[{\"id\":\"1\",\"type\":\"count\",\"schema\":\"metric\",\"params\":{}}],\"listeners\":{}}",
                "uiStateJSON": "{}",
                "description": "",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"index\":\"monitoring\",\"query\":{\"query_string\":{\"query\":\"type:3\",\"analyze_wildcard\":true}},\"filter\":[]}"
                }
            },
            'tasks_per_minute_bar': {
                "title": "tasks_per_minute_bar",
                "visState": "{\"title\":\"tasks_per_minute_bar\",\"type\":\"histogram\",\"params\":{\"shareYAxis\":true,\"addTooltip\":true,\"addLegend\":true,\"scale\":\"linear\",\"mode\":\"stacked\",\"times\":[],\"addTimeMarker\":false,\"defaultYExtents\":false,\"setYExtents\":false,\"yAxis\":{}},\"aggs\":[{\"id\":\"1\",\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"type\":\"date_histogram\",\"schema\":\"segment\",\"params\":{\"field\":\"date\",\"interval\":\"m\",\"customInterval\":\"2h\",\"min_doc_count\":1,\"extended_bounds\":{}}}],\"listeners\":{}}",
                "uiStateJSON": "{}",
                "description": "",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"index\":\"monitoring\",\"query\":{\"query_string\":{\"query\":\"*\",\"analyze_wildcard\":true}},\"filter\":[]}"
                }
            },
        }

        for visualization_name, visualization_json in visualizations.items():
            requests.post(
                url='http://{kibana_host}:{kibana_port}/elasticsearch/.kibana/visualization/{visualization_name}?op_type=create'.format(
                    kibana_host=kibana_host,
                    kibana_port=kibana_port,
                    visualization_name=visualization_name,
                ),
                json=visualization_json,
                headers={
                    'kbn-version': kibana_version,
                },
            )

    def _install_kibana_dashboard(self, kibana_host, kibana_port, kibana_version):
        requests.post(
            url='http://{kibana_host}:{kibana_port}/elasticsearch/.kibana/dashboard/tasks_dashboard?op_type=create'.format(
                kibana_host=kibana_host,
                kibana_port=kibana_port,
            ),
            json={
                "title": "tasks_dashboard",
                "hits": 0,
                "description": "",
                "panelsJSON": "[{\"col\":1,\"id\":\"total_success_counter\",\"panelIndex\":1,\"row\":1,\"size_x\":4,\"size_y\":2,\"type\":\"visualization\"},{\"col\":9,\"id\":\"total_retry_counter\",\"panelIndex\":2,\"row\":1,\"size_x\":4,\"size_y\":2,\"type\":\"visualization\"},{\"col\":5,\"id\":\"total_failure_counter\",\"panelIndex\":3,\"row\":1,\"size_x\":4,\"size_y\":2,\"type\":\"visualization\"},{\"col\":1,\"id\":\"tasks_per_minute_bar\",\"panelIndex\":4,\"row\":3,\"size_x\":12,\"size_y\":5,\"type\":\"visualization\"}]",
                "optionsJSON": "{\"darkTheme\":false}",
                "uiStateJSON": "{}",
                "version": 1,
                "timeRestore": False,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"filter\":[{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}}}]}"
                }
            },
            headers={
                'kbn-version': kibana_version,
            },
        )
