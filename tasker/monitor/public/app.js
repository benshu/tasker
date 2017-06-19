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
            .when(
                '/workers',
                {
                    templateUrl: 'pages/workers.html'
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
            $scope.metrics = {
                'counter': {
                    'process': 0,
                    'success': 0,
                    'retry': 0,
                    'failure': 0
                },
                'rate': {
                    'process': 0,
                    'success': 0,
                    'retry': 0,
                    'failure': 0,
                }
            };
            var domain = location.hostname + (location.port ? ':' + location.port: '')
            var websocket = new WebSocket('ws://' + domain + '/ws/statistics');

            websocket.onopen = function(event) {
                websocket.send('metrics');
                websocket.send('queues');
            };
            websocket.onmessage = function(event) {
                var message = JSON.parse(event.data);

                if (message.type == 'metrics') {
                    $scope.metrics = message.data;
                } else if (message.type == 'queues') {
                    $scope.queues = message.data;
                    console.log(message.data);
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
                },
                2000
            );
        }
    ]
);

TaskerDashboard.controller(
    'WorkersController',
    [
        '$scope',
        '$location',
        '$interval',
        function WorkersController($scope, $location, $interval) {
            $scope.workers_table_sort_by = 'hostname';
            $scope.workers_table_sort_by_reverse = true;

            var domain = location.hostname + (location.port ? ':' + location.port: '')
            var websocket = new WebSocket('ws://' + domain + '/ws/statistics');

            websocket.onopen = function(event) {
                websocket.send('workers');
            };
            websocket.onmessage = function(event) {
                var message = JSON.parse(event.data);

                if (message.type == 'workers') {
                    $scope.workers = message.data;
                }
            };

            $interval(
                function() {
                    websocket.send('workers');
                },
                2000
            );
        }
    ]
);
