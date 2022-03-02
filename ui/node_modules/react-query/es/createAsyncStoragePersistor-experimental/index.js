function _await(value, then, direct) {
  if (direct) {
    return then ? then(value) : value;
  }

  if (!value || !value.then) {
    value = Promise.resolve(value);
  }

  return then ? value.then(then) : value;
}

function _async(f) {
  return function () {
    for (var args = [], i = 0; i < arguments.length; i++) {
      args[i] = arguments[i];
    }

    try {
      return Promise.resolve(f.apply(this, args));
    } catch (e) {
      return Promise.reject(e);
    }
  };
}

function _empty() {}

function _invokeIgnored(body) {
  var result = body();

  if (result && result.then) {
    return result.then(_empty);
  }
}

export var createAsyncStoragePersistor = function createAsyncStoragePersistor(_ref) {
  var storage = _ref.storage,
      _ref$key = _ref.key,
      key = _ref$key === void 0 ? "REACT_QUERY_OFFLINE_CACHE" : _ref$key,
      _ref$throttleTime = _ref.throttleTime,
      throttleTime = _ref$throttleTime === void 0 ? 1000 : _ref$throttleTime,
      _ref$serialize = _ref.serialize,
      serialize = _ref$serialize === void 0 ? JSON.stringify : _ref$serialize,
      _ref$deserialize = _ref.deserialize,
      deserialize = _ref$deserialize === void 0 ? JSON.parse : _ref$deserialize;
  return {
    persistClient: asyncThrottle(function (persistedClient) {
      return storage.setItem(key, serialize(persistedClient));
    }, {
      interval: throttleTime
    }),
    restoreClient: _async(function () {
      return _await(storage.getItem(key), function (cacheString) {
        if (!cacheString) {
          return;
        }

        return deserialize(cacheString);
      });
    }),
    removeClient: function removeClient() {
      return storage.removeItem(key);
    }
  };
};

function asyncThrottle(func, _temp) {
  var _ref2 = _temp === void 0 ? {} : _temp,
      _ref2$interval = _ref2.interval,
      interval = _ref2$interval === void 0 ? 1000 : _ref2$interval,
      _ref2$limit = _ref2.limit,
      limit = _ref2$limit === void 0 ? 1 : _ref2$limit;

  if (typeof func !== 'function') throw new Error('argument is not function.');
  var running = {
    current: false
  };
  var lastTime = 0;
  var timeout;
  var queue = [];
  return function () {
    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    return _async(function () {
      if (running.current) {
        lastTime = Date.now();

        if (queue.length > limit) {
          queue.shift();
        }

        queue.push(args);
        clearTimeout(timeout);
      }

      return _invokeIgnored(function () {
        if (Date.now() - lastTime > interval) {
          running.current = true;
          return _await(func.apply(void 0, args), function () {
            lastTime = Date.now();
            running.current = false;
          });
        } else {
          if (queue.length > 0) {
            var lastArgs = queue[queue.length - 1];
            timeout = setTimeout(_async(function () {
              return _invokeIgnored(function () {
                if (!running.current) {
                  running.current = true;
                  return _await(func.apply(void 0, lastArgs), function () {
                    running.current = false;
                  });
                }
              });
            }), interval);
          }
        }
      });
    })();
  };
}