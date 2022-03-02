import { FetchNextPageOptions, FetchPreviousPageOptions, InfiniteData, InfiniteQueryObserverOptions, InfiniteQueryObserverResult } from './types';
import { QueryClient } from './queryClient';
import { NotifyOptions, ObserverFetchOptions, QueryObserver } from './queryObserver';
import { Query } from './query';
declare type InfiniteQueryObserverListener<TData, TError> = (result: InfiniteQueryObserverResult<TData, TError>) => void;
export declare class InfiniteQueryObserver<TQueryFnData = unknown, TError = unknown, TData = TQueryFnData, TQueryData = TQueryFnData> extends QueryObserver<TQueryFnData, TError, InfiniteData<TData>, InfiniteData<TQueryData>> {
    subscribe: (listener?: InfiniteQueryObserverListener<TData, TError>) => () => void;
    getCurrentResult: () => InfiniteQueryObserverResult<TData, TError>;
    protected fetch: (fetchOptions?: ObserverFetchOptions) => Promise<InfiniteQueryObserverResult<TData, TError>>;
    constructor(client: QueryClient, options: InfiniteQueryObserverOptions<TQueryFnData, TError, TData, TQueryData>);
    protected bindMethods(): void;
    setOptions(options?: InfiniteQueryObserverOptions<TQueryFnData, TError, TData, TQueryData>, notifyOptions?: NotifyOptions): void;
    getOptimisticResult(options: InfiniteQueryObserverOptions<TQueryFnData, TError, TData, TQueryData>): InfiniteQueryObserverResult<TData, TError>;
    fetchNextPage(options?: FetchNextPageOptions): Promise<InfiniteQueryObserverResult<TData, TError>>;
    fetchPreviousPage(options?: FetchPreviousPageOptions): Promise<InfiniteQueryObserverResult<TData, TError>>;
    protected createResult(query: Query<TQueryFnData, TError, InfiniteData<TQueryData>>, options: InfiniteQueryObserverOptions<TQueryFnData, TError, TData, TQueryData>): InfiniteQueryObserverResult<TData, TError>;
}
export {};
