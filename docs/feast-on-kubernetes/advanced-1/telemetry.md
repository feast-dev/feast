# Telemetry

## How telemetry is used

The Feast maintainers use anonymous usage statistics and error tracking to help shape the Feast roadmap. Several client methods are tracked, beginning in Feast 0.9. Users are assigned a UUID which is sent along with the name of the method, the Feast version, the OS \(using `sys.platform`\), and the current time. For more detailed information see [the source code](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/telemetry.py).

## How to disable telemetry

To opt out of all telemetry, simply set the environment variable `FEAST_TELEMETRY` to `False` in the environment in which the Feast client is run.

