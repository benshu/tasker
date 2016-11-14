var TaskerDashboard = angular.module(
    'TaskerDashboard',
    [
        'ngRoute',
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
            .otherwise(
                {
                    redirectTo: '/dashboard'
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
        '$location',
        '$interval',
        function WorkersController($scope, $location, $interval) {
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

            $interval(
                function() {
                    websocket.emit(
                        'workers',
                        {}
                    );
                },
                2000
            );
        }
    ]
);
