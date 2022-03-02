import { BroadcastChannel } from 'broadcast-channel';
export function broadcastQueryClient(_ref) {
  var queryClient = _ref.queryClient,
      _ref$broadcastChannel = _ref.broadcastChannel,
      broadcastChannel = _ref$broadcastChannel === void 0 ? 'react-query' : _ref$broadcastChannel;
  var transaction = false;

  var tx = function tx(cb) {
    transaction = true;
    cb();
    transaction = false;
  };

  var channel = new BroadcastChannel(broadcastChannel, {
    webWorkerSupport: false
  });
  var queryCache = queryClient.getQueryCache();
  queryClient.getQueryCache().subscribe(function (queryEvent) {
    var _queryEvent$action;

    if (transaction || !(queryEvent == null ? void 0 : queryEvent.query)) {
      return;
    }

    var _queryEvent$query = queryEvent.query,
        queryHash = _queryEvent$query.queryHash,
        queryKey = _queryEvent$query.queryKey,
        state = _queryEvent$query.state;

    if (queryEvent.type === 'queryUpdated' && ((_queryEvent$action = queryEvent.action) == null ? void 0 : _queryEvent$action.type) === 'success') {
      channel.postMessage({
        type: 'queryUpdated',
        queryHash: queryHash,
        queryKey: queryKey,
        state: state
      });
    }

    if (queryEvent.type === 'queryRemoved') {
      channel.postMessage({
        type: 'queryRemoved',
        queryHash: queryHash,
        queryKey: queryKey
      });
    }
  });

  channel.onmessage = function (action) {
    if (!(action == null ? void 0 : action.type)) {
      return;
    }

    tx(function () {
      var type = action.type,
          queryHash = action.queryHash,
          queryKey = action.queryKey,
          state = action.state;

      if (type === 'queryUpdated') {
        var query = queryCache.get(queryHash);

        if (query) {
          query.setState(state);
          return;
        }

        queryCache.build(queryClient, {
          queryKey: queryKey,
          queryHash: queryHash
        }, state);
      } else if (type === 'queryRemoved') {
        var _query = queryCache.get(queryHash);

        if (_query) {
          queryCache.remove(_query);
        }
      }
    });
  };
}