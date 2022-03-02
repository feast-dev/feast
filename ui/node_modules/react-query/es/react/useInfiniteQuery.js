import { InfiniteQueryObserver } from '../core/infiniteQueryObserver';
import { parseQueryArgs } from '../core/utils';
import { useBaseQuery } from './useBaseQuery'; // HOOK

export function useInfiniteQuery(arg1, arg2, arg3) {
  var options = parseQueryArgs(arg1, arg2, arg3);
  return useBaseQuery(options, InfiniteQueryObserver);
}