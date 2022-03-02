import _extends from "@babel/runtime/helpers/esm/extends";
import { noop } from '../core/utils';
export function createWebStoragePersistor(_ref) {
  var storage = _ref.storage,
      _ref$key = _ref.key,
      key = _ref$key === void 0 ? "REACT_QUERY_OFFLINE_CACHE" : _ref$key,
      _ref$throttleTime = _ref.throttleTime,
      throttleTime = _ref$throttleTime === void 0 ? 1000 : _ref$throttleTime,
      _ref$serialize = _ref.serialize,
      serialize = _ref$serialize === void 0 ? JSON.stringify : _ref$serialize,
      _ref$deserialize = _ref.deserialize,
      deserialize = _ref$deserialize === void 0 ? JSON.parse : _ref$deserialize;

  //try to save data to storage
  function trySave(persistedClient) {
    try {
      storage.setItem(key, serialize(persistedClient));
    } catch (_unused) {
      return false;
    }

    return true;
  }

  if (typeof storage !== 'undefined') {
    return {
      persistClient: throttle(function (persistedClient) {
        if (trySave(persistedClient) !== true) {
          var mutations = [].concat(persistedClient.clientState.mutations);
          var queries = [].concat(persistedClient.clientState.queries);

          var _client = _extends({}, persistedClient, {
            clientState: {
              mutations: mutations,
              queries: queries
            }
          }); // sort queries by dataUpdatedAt (oldest first)


          var sortedQueries = [].concat(queries).sort(function (a, b) {
            return a.state.dataUpdatedAt - b.state.dataUpdatedAt;
          }); // clean old queries and try to save

          var _loop = function _loop() {
            var oldestData = sortedQueries.shift();
            _client.clientState.queries = queries.filter(function (q) {
              return q !== oldestData;
            });

            if (trySave(_client)) {
              return {
                v: void 0
              }; // save success
            }
          };

          while (sortedQueries.length > 0) {
            var _ret = _loop();

            if (typeof _ret === "object") return _ret.v;
          } // clean mutations and try to save


          while (mutations.shift()) {
            if (trySave(_client)) {
              return; // save success
            }
          }
        }
      }, throttleTime),
      restoreClient: function restoreClient() {
        var cacheString = storage.getItem(key);

        if (!cacheString) {
          return;
        }

        return deserialize(cacheString);
      },
      removeClient: function removeClient() {
        storage.removeItem(key);
      }
    };
  }

  return {
    persistClient: noop,
    restoreClient: noop,
    removeClient: noop
  };
}

function throttle(func, wait) {
  if (wait === void 0) {
    wait = 100;
  }

  var timer = null;
  var params;
  return function () {
    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    params = args;

    if (timer === null) {
      timer = setTimeout(function () {
        func.apply(void 0, params);
        timer = null;
      }, wait);
    }
  };
}