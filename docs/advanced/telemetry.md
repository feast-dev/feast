# Telemetry

{% hint style="danger" %}
We strongly encourage all users to upgrade from Feast 0.9 to Feast 0.10+. Please see [this](https://docs.feast.dev/v/master/project/feast-0.9-vs-feast-0.10+) for an explanation of the differences between the two versions. A guide to upgrading can be found [here](https://docs.google.com/document/d/1AOsr_baczuARjCpmZgVd8mCqTF4AZ49OEyU4Cn-uTT0/edit#heading=h.9gb2523q4jlh). 
{% endhint %}

## How telemetry is used

The Feast maintainers use anonymous usage statistics to help shape the Feast roadmap. Several client methods are tracked, beginning in Feast 0.9. Users are assigned a UUID which is sent along with the name of the method, the Feast version, the OS \(using `sys.platform`\), and the current time. For more detailed information see [the source code](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/telemetry.py).

## How to disable telemetry

To opt out of telemetry, simply set the environment variable `FEAST_TELEMETRY` to `False` in the environment in which the Feast client is run.

