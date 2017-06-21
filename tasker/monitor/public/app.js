var TaskerDashboard = angular.module(
    'TaskerDashboard',
    [
        'ngRoute',
    ]
);
TaskerDashboard.config(
    [
        '$routeProvider',
        '$locationProvider',
        function($routeProvider, $locationProvider) {
            $locationProvider
            .hashPrefix('!');

            $routeProvider
            .when(
                '/dashboard',
                {
                    templateUrl: 'pages/dashboard.html'
                }
            )
            .otherwise(
                {
                    redirectTo: 'dashboard'
                }
            );
        }
    ]
);

TaskerDashboard.controller(
    'DashboardController',
    [
        '$scope',
        '$location',
        '$interval',
        function DashboardController($scope, $location, $interval) {
            var domain = location.hostname + (location.port ? ':' + location.port: '')
            var websocket = new WebSocket('ws://' + domain + '/ws/statistics');

            $scope.websocket_connected = false;
            $scope.metrics = {
                'process': 0,
                'success': 0,
                'retry': 0,
                'failure': 0
            };
            $scope.rates = {
                'process_per_second': 0,
                'success_per_second': 0,
                'retry_per_second': 0,
                'failure_per_second': 0
            };
            $scope.statistics = {};
            $scope.workers = [];
            $scope.queues = {};

            $scope.workers_table_sort_by = 'hostname';
            $scope.workers_table_sort_by_reverse = true;

            websocket.onclose = function(event) {
                $scope.websocket_connected = false;
            }
            websocket.onopen = function(event) {
                $scope.websocket_connected = true;
                websocket.send('metrics');
                websocket.send('queues');
                websocket.send('workers');
            };
            websocket.onmessage = function(event) {
                var message = JSON.parse(event.data);

                if (message.type == 'metrics') {
                    $scope.metrics = message.data.metrics;
                    $scope.rates = message.data.rates;
                } else if (message.type == 'queues') {
                    $scope.queues = message.data;
                } else if (message.type == 'workers') {
                    $scope.workers = message.data;

                    var statistics = {};

                    for (var i = 0; i < $scope.workers.length; i++) {
                        var current_worker = $scope.workers[i];

                        if (!(current_worker.name in statistics)) {
                            statistics[current_worker.name] = {
                                'process': 0,
                                'success': 0,
                                'retry': 0,
                                'failure': 0
                            };
                        }

                        statistics[current_worker.name].process += current_worker.metrics.process;
                        statistics[current_worker.name].success += current_worker.metrics.success;
                        statistics[current_worker.name].retry += current_worker.metrics.retry;
                        statistics[current_worker.name].failure += current_worker.metrics.failure;
                    }

                    $scope.statistics = statistics;
                }
            };

            $interval(
                function() {
                    websocket.send('metrics');
                },
                1000
            );
            $interval(
                function() {
                    websocket.send('queues');
                    websocket.send('workers');
                },
                2000
            );
        }
    ]
);
