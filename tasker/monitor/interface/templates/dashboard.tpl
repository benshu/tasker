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
                <h3 style="text-align: center; font-weight: bold;">Dashboard</h4>
            </div>
        </div>
        <hr>
        <div class="row">
            <div class="four columns">
                <h4 style="text-align: center; font-weight: bold;">Success</h4>
                <h5 style="color: #009688; text-align: center; font-weight: bold;">{{ statistics['success'] }}</h5>
            </div>
            <div class="four columns">
                <h4 style="text-align: center; font-weight: bold;">Retry</h4>
                <h5 style="color: #03A9F4; text-align: center; font-weight: bold;">{{ statistics['retry'] }}</h5>
            </div>
            <div class="four columns">
                <h4 style="text-align: center; font-weight: bold;">Failure</h4>
                <h5 style="color: #E53935; text-align: center; font-weight: bold;">{{ statistics['failure'] }}</h5>
            </div>
        </div>
        <hr>
        <div class="row">
            <div class="six columns">
                <div class="row">
                    <h5 style="color: #009688; text-align: center; font-weight: bold;">Online</h5>
                </div>
                <div class="row">
                    <table class="u-full-width">
                        <thead>
                            <tr>
                                <th>Host</th>
                                <th>Worker</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td>host1.domain.com</td>
                                <td>add</td>
                            </tr>
                            <tr>
                                <td>host2.domain.com</td>
                                <td>add</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
            <div class="six columns">
                <div class="row">
                    <h5 style="color: #E53935; text-align: center; font-weight: bold;">Offline</h5>
                </div>
                <div class="row">
                    <table class="u-full-width">
                        <thead>
                            <tr>
                                <th>Host</th>
                                <th>Worker</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td>host1.domain.com</td>
                                <td>remove</td>
                            </tr>
                            <tr>
                                <td>host2.domain.com</td>
                                <td>remove</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
</body>

</html>
