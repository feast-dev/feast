import { getLogger } from '../core/logger';
import { dehydrate, hydrate } from 'react-query';

function _await(value, then, direct) {
  if (direct) {
    return then ? then(value) : value;
  }

  if (!value || !value.then) {
    value = Promise.resolve(value);
  }

  return then ? value.then(then) : value;
}

function _catch(body, recover) {
  try {
    var result = body();
  } catch (e) {
    return recover(e);
  }

  if (result && result.then) {
    return result.then(void 0, recover);
  }

  return result;
}

function _continue(value, then) {
  return value && value.then ? value.then(then) : then(value);
}

function _empty() {}

function _invokeIgnored(body) {
  var result = body();

  if (result && result.then) {
    return result.then(_empty);
  }
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

export var persistQueryClient = _async(function (_ref) {
  var queryClient = _ref.queryClient,
      persistor = _ref.persistor,
      _ref$maxAge = _ref.maxAge,
      maxAge = _ref$maxAge === void 0 ? 1000 * 60 * 60 * 24 : _ref$maxAge,
      _ref$buster = _ref.buster,
      buster = _ref$buster === void 0 ? '' : _ref$buster,
      hydrateOptions = _ref.hydrateOptions,
      dehydrateOptions = _ref.dehydrateOptions;
  return _invokeIgnored(function () {
    if (typeof window !== 'undefined') {
      // Subscribe to changes
      var saveClient = function saveClient() {
        var persistClient = {
          buster: buster,
          timestamp: Date.now(),
          clientState: dehydrate(queryClient, dehydrateOptions)
        };
        persistor.persistClient(persistClient);
      }; // Attempt restore


      return _continue(_catch(function () {
        return _await(persistor.restoreClient(), function (persistedClient) {
          if (persistedClient) {
            if (persistedClient.timestamp) {
              var expired = Date.now() - persistedClient.timestamp > maxAge;
              var busted = persistedClient.buster !== buster;

              if (expired || busted) {
                persistor.removeClient();
              } else {
                hydrate(queryClient, persistedClient.clientState, hydrateOptions);
              }
            } else {
              persistor.removeClient();
            }
          }
        });
      }, function (err) {
        getLogger().error(err);
        getLogger().warn('Encountered an error attempting to restore client cache from persisted location. As a precaution, the persisted cache will be discarded.');
        persistor.removeClient();
      }), function () {
        // Subscribe to changes in the query cache to trigger the save
        queryClient.getQueryCache().subscribe(saveClient);
      });
    }
  });
});