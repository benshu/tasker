<!DOCTYPE html>
<html lang="en">
    <head>
        <title>Tasker Statistics Dashboard</title>

        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">

        <link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Roboto:400,300,600">
        <link rel="stylesheet" href="https://fonts.googleapis.com/icon?family=Material+Icons">
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/materialize/0.97.6/css/materialize.min.css">
        <script src="https://code.jquery.com/jquery-2.2.3.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/materialize/0.97.6/js/materialize.min.js"></script>
    </head>

    <body>
        <div class="container">
            <div class="row">
                <div class="col s12">
                    <h3 style="text-align: center; font-weight: bold;">Dashboard</h3>
                </div>
            </div>
            <hr>
            <div class="row">
                <div class="col s4">
                    <h4 style="text-align: center; font-weight: bold;">Success</h4>
                    <h5 style="color: #009688; text-align: center; font-weight: bold;">{{ '{:,}'.format(statistics['success']) }}</h5>
                </div>
                <div class="col s4">
                    <h4 style="text-align: center; font-weight: bold;">Retry</h4>
                    <h5 style="color: #03A9F4; text-align: center; font-weight: bold;">{{ '{:,}'.format(statistics['retry']) }}</h5>
                </div>
                <div class="col s4">
                    <h4 style="text-align: center; font-weight: bold;">Failure</h4>
                    <h5 style="color: #E53935; text-align: center; font-weight: bold;">{{ '{:,}'.format(statistics['failure']) }}</h5>
                </div>
            </div>
            <hr>
            <div class="row">
                <div class="col s12">
                    <div class="row">
                        <table class="bordered stripped">
                            <thead>
                                <tr>
                                    <th>Hostname</th>
                                    <th>Worker</th>
                                    <th>Success</th>
                                    <th>Retry</th>
                                    <th>Fail</th>
                                </tr>
                            </thead>
                            <tbody>
                                {% for worker in statistics['online_workers'] | sort(attribute='worker_name') %}
                                    <tr>
                                        <td style="font-weight: bold;">{{ worker['hostname'] }}</td>
                                        <td style="font-weight: bold;">{{ worker['worker_name'] }}</td>
                                        <td style="color: #009688; font-weight: bold;">{{ worker['success'] }}</td>
                                        <td style="color: #03A9F4; font-weight: bold;">{{ worker['retry'] }}</td>
                                        <td style="color: #E53935; font-weight: bold;">{{ worker['failure'] }}</td>
                                    </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    </body>
</html>
