# Telemetry

### How telemetry is used

The Feast project logs anonymous usage statistics and errors in order to inform our planning. Several client methods are tracked, beginning in Feast 0.9. Users are assigned a UUID which is sent along with the name of the method, the Feast version, the OS \(using `sys.platform`\), and the current time.

The [source code](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/telemetry.py) is available here.

### How to disable telemetry

Set the environment variable `FEAST_TELEMETRY` to `False`.

