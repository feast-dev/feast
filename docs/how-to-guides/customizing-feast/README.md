# Customizing Feast

Feast is highly pluggable and configurable:

* One can use existing plugins (offline store, online store, batch materialization engine, providers) and configure those using the built in options. See reference documentation for details.
* The other way to customize Feast is to build your own custom components, and then point Feast to delegate to them.

Below are some guides on how to add new custom components:

{% content-ref url="adding-a-new-offline-store.md" %}
[adding-a-new-offline-store.md](adding-a-new-offline-store.md)
{% endcontent-ref %}

{% content-ref url="adding-support-for-a-new-online-store.md" %}
[adding-support-for-a-new-online-store.md](adding-support-for-a-new-online-store.md)
{% endcontent-ref %}

{% content-ref url="creating-a-custom-materialization-engine.md" %}
[creating-a-custom-materialization-engine.md](creating-a-custom-materialization-engine.md)
{% endcontent-ref %}

{% content-ref url="creating-a-custom-provider.md" %}
[creating-a-custom-provider.md](creating-a-custom-provider.md)
{% endcontent-ref %}
