# Usage

## How Feast SDK usage is measured

The Feast project logs anonymous usage statistics and errors in order to inform our planning. Several client methods are tracked, beginning in Feast 0.9. Users are assigned a UUID which is sent along with the name of the method, the Feast version, the OS \(using `sys.platform`\), and the current time.

The [source code](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/usage.py) is available here.

## How to disable usage logging

Set the environment variable `FEAST_USAGE` to `False`.

