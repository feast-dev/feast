import React from 'react';
import { notifyManager } from '../core/notifyManager';
import { parseFilterArgs } from '../core/utils';
import { useQueryClient } from './QueryClientProvider';
export function useIsFetching(arg1, arg2) {
  var mountedRef = React.useRef(false);
  var queryClient = useQueryClient();

  var _parseFilterArgs = parseFilterArgs(arg1, arg2),
      filters = _parseFilterArgs[0];

  var _React$useState = React.useState(queryClient.isFetching(filters)),
      isFetching = _React$useState[0],
      setIsFetching = _React$useState[1];

  var filtersRef = React.useRef(filters);
  filtersRef.current = filters;
  var isFetchingRef = React.useRef(isFetching);
  isFetchingRef.current = isFetching;
  React.useEffect(function () {
    mountedRef.current = true;
    var unsubscribe = queryClient.getQueryCache().subscribe(notifyManager.batchCalls(function () {
      if (mountedRef.current) {
        var newIsFetching = queryClient.isFetching(filtersRef.current);

        if (isFetchingRef.current !== newIsFetching) {
          setIsFetching(newIsFetching);
        }
      }
    }));
    return function () {
      mountedRef.current = false;
      unsubscribe();
    };
  }, [queryClient]);
  return isFetching;
}