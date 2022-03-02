import { QueryKey } from '../core';
import { QueryObserver } from '../core/queryObserver';
import { UseBaseQueryOptions } from './types';
export declare function useBaseQuery<TQueryFnData, TError, TData, TQueryData, TQueryKey extends QueryKey>(options: UseBaseQueryOptions<TQueryFnData, TError, TData, TQueryData, TQueryKey>, Observer: typeof QueryObserver): import("../core").QueryObserverResult<TData, TError>;
