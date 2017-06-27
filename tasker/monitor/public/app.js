var taskerDashboard = angular.module(
    "taskerDashboard",
    [
        "ngRoute",
    ]
);
taskerDashboard.config(
    [
        "$routeProvider",
        "$locationProvider",
        function($routeProvider, $locationProvider) {
            $locationProvider
            .hashPrefix("!");

            $routeProvider
            .when(
                "/dashboard",
                {
                    templateUrl: "pages/dashboard.html"
                }
            )
            .otherwise(
                {
                    redirectTo: "dashboard"
                }
            );
        }
    ]
);

taskerDashboard.controller(
    "dashboardController",
    [
        "$scope",
        "$location",
        "$interval",
        function dashboardController($scope, $location, $interval) {
            var domain = location.hostname + (location.port ? ":" + location.port: "");
            var websocket = new WebSocket("ws://" + domain + "/ws/statistics");

            $scope.websocketConnected = false;
            $scope.metrics = {
                "process": 0,
                "success": 0,
                "retry": 0,
                "failure": 0
            };
            $scope.rates = {
                "process_per_second": 0,
                "success_per_second": 0,
                "retry_per_second": 0,
                "failure_per_second": 0
            };
            $scope.statistics = {};
            $scope.workers = [];
            $scope.queues = {};

            $scope.workersTableSortBy = "hostname";
            $scope.workersTableSortByReverse = true;

            websocket.onclose = function(event) {
                $scope.websocketConnected = false;
            };
            websocket.onopen = function(event) {
                $scope.websocketConnected = true;
                websocket.send("metrics");
                websocket.send("queues");
                websocket.send("workers");
            };
            websocket.onmessage = function(event) {
                var message = JSON.parse(event.data);

                if (message.type === "metrics") {
                    $scope.metrics = message.data.metrics;
                    $scope.rates = message.data.rates;
                } else if (message.type === "queues") {
                    $scope.queues = message.data;
                } else if (message.type === "workers") {
                    $scope.workers = message.data;

                    var statistics = {};

                    for (var i = 0; i < $scope.workers.length; i++) {
                        var currentWorker = $scope.workers[i];

                        if (!(currentWorker.name in statistics)) {
                            statistics[currentWorker.name] = {
                                "process": 0,
                                "success": 0,
                                "retry": 0,
                                "failure": 0
                            };
                        }

                        statistics[currentWorker.name].process += currentWorker.metrics.process;
                        statistics[currentWorker.name].success += currentWorker.metrics.success;
                        statistics[currentWorker.name].retry += currentWorker.metrics.retry;
                        statistics[currentWorker.name].failure += currentWorker.metrics.failure;
                    }

                    $scope.statistics = statistics;
                }
            };

            $interval(
                function() {
                    websocket.send("metrics");
                },
                1000
            );
            $interval(
                function() {
                    websocket.send("queues");
                    websocket.send("workers");
                },
                2000
            );
        }
    ]
);
