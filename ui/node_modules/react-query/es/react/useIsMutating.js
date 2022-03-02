import React from 'react';
import { notifyManager } from '../core/notifyManager';
import { parseMutationFilterArgs } from '../core/utils';
import { useQueryClient } from './QueryClientProvider';
export function useIsMutating(arg1, arg2) {
  var mountedRef = React.useRef(false);
  var filters = parseMutationFilterArgs(arg1, arg2);
  var queryClient = useQueryClient();

  var _React$useState = React.useState(queryClient.isMutating(filters)),
      isMutating = _React$useState[0],
      setIsMutating = _React$useState[1];

  var filtersRef = React.useRef(filters);
  filtersRef.current = filters;
  var isMutatingRef = React.useRef(isMutating);
  isMutatingRef.current = isMutating;
  React.useEffect(function () {
    mountedRef.current = true;
    var unsubscribe = queryClient.getMutationCache().subscribe(notifyManager.batchCalls(function () {
      if (mountedRef.current) {
        var newIsMutating = queryClient.isMutating(filtersRef.current);

        if (isMutatingRef.current !== newIsMutating) {
          setIsMutating(newIsMutating);
        }
      }
    }));
    return function () {
      mountedRef.current = false;
      unsubscribe();
    };
  }, [queryClient]);
  return isMutating;
}