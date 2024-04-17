# Usage

## How Feast SDK usage is measured

The Feast project has a feature to log usage statistics and errors. Several client methods are tracked, beginning in Feast 0.9. Users are assigned a UUID which is sent along with the name of the method, the Feast version, the OS \(using `sys.platform`\), and the current time.

The [source code](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/usage.py) is available here.

## How to enable the usage logging

Set the environment variable `FEAST_USAGE` to `True` (in String type) and config your endpoint by the variable `FEAST_USAGE_ENDPOINT`.  

