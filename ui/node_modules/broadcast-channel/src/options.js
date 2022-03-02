export function fillOptionsWithDefaults(originalOptions = {}) {
    const options = JSON.parse(JSON.stringify(originalOptions));

    // main
    if (typeof options.webWorkerSupport === 'undefined') options.webWorkerSupport = true;


    // indexed-db
    if (!options.idb) options.idb = {};
    //  after this time the messages get deleted
    if (!options.idb.ttl) options.idb.ttl = 1000 * 45;
    if (!options.idb.fallbackInterval) options.idb.fallbackInterval = 150;
    //  handles abrupt db onclose events.
    if (originalOptions.idb && typeof originalOptions.idb.onclose === 'function')
        options.idb.onclose = originalOptions.idb.onclose;

    // localstorage
    if (!options.localstorage) options.localstorage = {};
    if (!options.localstorage.removeTimeout) options.localstorage.removeTimeout = 1000 * 60;

    // custom methods
    if (originalOptions.methods) options.methods = originalOptions.methods;

    // node
    if (!options.node) options.node = {};
    if (!options.node.ttl) options.node.ttl = 1000 * 60 * 2; // 2 minutes;
    if (typeof options.node.useFastPath === 'undefined') options.node.useFastPath = true;

    return options;
}
