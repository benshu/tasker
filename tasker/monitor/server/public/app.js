var TaskerDashboard = angular.module(
    'TaskerDashboard',
    [
        'ngRoute',
        'ngWebSocket',
    ]
);
TaskerDashboard.config(
    [
        '$routeProvider',
        function($routeProvider) {
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
            .when(
                '/queues',
                {
                    templateUrl: 'pages/queues.html'
                }
            )
            .otherwise(
                {
                    redirectTo: '/dashboard'
                }
            )
        }
    ]
);

TaskerDashboard.controller(
    'DashboardController',
    [
        '$scope',
        '$websocket',
        '$location',
        '$interval',
        function DashboardController($scope, $websocket, $location, $interval) {
            $scope.statistics = {
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
            var host = $location.host();
            var websocket = io(
                {
                    'port': 8000
                }
            );

            websocket.on(
                'statistics',
                function(data) {
                    $scope.statistics = data;
                }
            );

            $interval(
                function() {
                    websocket.emit(
                        'statistics',
                        {}
                    );
                },
                1000
            );
        }
    ]
);

TaskerDashboard.controller(
    'QueuesController',
    [
        '$scope',
        '$websocket',
        '$location',
        '$interval',
        function QueuesController($scope, $websocket, $location, $interval) {
            var host = $location.host();
            var websocket = io(
                {
                    'port': 8000
                }
            );

            websocket.on(
                'queues',
                function(data) {
                    $scope.queues = data;
                }
            );

            $interval(
                function() {
                    websocket.emit(
                        'queues',
                        {}
                    );
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
        '$websocket',
        '$location',
        '$interval',
        function WorkersController($scope, $websocket, $location, $interval) {
            $scope.workers_table_sort_by = 'hostname';
            $scope.workers_table_sort_by_reverse = true;

            var host = $location.host();
            var websocket = io(
                {
                    'port': 8000
                }
            );

            websocket.on(
                'workers',
                function(data) {
                    $scope.workers = data;
                }
            );

            $scope.update_workers = function() {
                websocket.emit(
                    'workers',
                    {}
                );
            };
        }
    ]
);
