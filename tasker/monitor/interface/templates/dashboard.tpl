<!DOCTYPE html>
<html lang="en">
    <head>
        <meta charset="utf-8">
        <title>Tasker Statistics Dashboard</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link href="//fonts.googleapis.com/css?family=Roboto:400,300,600" rel="stylesheet" type="text/css">
        <link rel="stylesheet" href="css/normalize.css">
        <link rel="stylesheet" href="css/skeleton.css">
        <link rel="icon" type="image/png" href="images/favicon.png">
    </head>

    <body>
        <div class="container">
            <div class="row">
                <div class="twelve columns">
                    <h3 style="text-align: center; font-weight: bold;">Dashboard</h3>
                </div>
            </div>
            <hr>
            <div class="row">
                <div class="four columns">
                    <h4 style="text-align: center; font-weight: bold;">Success</h4>
                    <h5 style="color: #009688; text-align: center; font-weight: bold;">{{ '{:,}'.format(statistics['success']) }}</h5>
                </div>
                <div class="four columns">
                    <h4 style="text-align: center; font-weight: bold;">Retry</h4>
                    <h5 style="color: #03A9F4; text-align: center; font-weight: bold;">{{ '{:,}'.format(statistics['retry']) }}</h5>
                </div>
                <div class="four columns">
                    <h4 style="text-align: center; font-weight: bold;">Failure</h4>
                    <h5 style="color: #E53935; text-align: center; font-weight: bold;">{{ '{:,}'.format(statistics['failure']) }}</h5>
                </div>
            </div>
            <hr>
            <div class="row">
                <div class="twelve columns">
                    <div class="row">
                        <table class="u-full-width">
                            <thead>
                                <tr>
                                    <th>Worker</th>
                                    <th>Success</th>
                                    <th>Retry</th>
                                    <th>Fail</th>
                                </tr>
                            </thead>
                            <tbody>
                                {% for worker in statistics['online_workers'] | sort(attribute='name') %}
                                    <tr>
                                        <td style="font-weight: bold;">{{ worker['name'] }}</td>
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
