class Statistics:
    def __init__(
        self,
    ):
        self.metrics = {
            'success': 0,
            'failure': 0,
            'retry': 0,
            'process': 0,
            'heartbeat': 0,
        }

        self.workers = {}

    def process_report(
        self,
        message,
    ):
        self.process_report_metrics(
            message=message,
        )
        self.process_report_worker_statistics(
            message=message,
        )

    def process_report_metrics(
        self,
        message,
    ):
        report_type = message['type']
        report_value = message['value']

        self.metrics[report_type] += report_value

    def process_report_worker_statistics(
        self,
        message,
    ):
        report_hostname = message['hostname']
        report_worker_name = message['worker_name']
        report_type = message['type']
        report_value = message['value']

        if report_hostname not in self.workers:
            self.workers[report_hostname] = {
                report_worker_name: {
                    'success': 0,
                    'failure': 0,
                    'retry': 0,
                    'process': 0,
                    'heartbeat': 0,
                }
            }
        elif report_worker_name not in self.workers[report_hostname]:
            self.workers[report_hostname][report_worker_name] = {
                'success': 0,
                'failure': 0,
                'retry': 0,
                'process': 0,
                'heartbeat': 0,
            }

        self.workers[report_hostname][report_worker_name][report_type] += report_value
