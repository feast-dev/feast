import { Updater } from './utils';
import { QueryKey, QueryOptions, QueryStatus, EnsuredQueryKey, QueryMeta, CancelOptions, SetDataOptions } from './types';
import { QueryCache } from './queryCache';
import { QueryObserver } from './queryObserver';
interface QueryConfig<TQueryFnData, TError, TData, TQueryKey extends QueryKey = QueryKey> {
    cache: QueryCache;
    queryKey: TQueryKey;
    queryHash: string;
    options?: QueryOptions<TQueryFnData, TError, TData, TQueryKey>;
    defaultOptions?: QueryOptions<TQueryFnData, TError, TData, TQueryKey>;
    state?: QueryState<TData, TError>;
    meta: QueryMeta | undefined;
}
export interface QueryState<TData = unknown, TError = unknown> {
    data: TData | undefined;
    dataUpdateCount: number;
    dataUpdatedAt: number;
    error: TError | null;
    errorUpdateCount: number;
    errorUpdatedAt: number;
    fetchFailureCount: number;
    fetchMeta: any;
    isFetching: boolean;
    isInvalidated: boolean;
    isPaused: boolean;
    status: QueryStatus;
}
export interface FetchContext<TQueryFnData, TError, TData, TQueryKey extends QueryKey = QueryKey> {
    fetchFn: () => unknown | Promise<unknown>;
    fetchOptions?: FetchOptions;
    options: QueryOptions<TQueryFnData, TError, TData, any>;
    queryKey: EnsuredQueryKey<TQueryKey>;
    state: QueryState<TData, TError>;
    meta: QueryMeta | undefined;
}
export interface QueryBehavior<TQueryFnData = unknown, TError = unknown, TData = TQueryFnData, TQueryKey extends QueryKey = QueryKey> {
    onFetch: (context: FetchContext<TQueryFnData, TError, TData, TQueryKey>) => void;
}
export interface FetchOptions {
    cancelRefetch?: boolean;
    meta?: any;
}
interface FailedAction {
    type: 'failed';
}
interface FetchAction {
    type: 'fetch';
    meta?: any;
}
interface SuccessAction<TData> {
    data: TData | undefined;
    type: 'success';
    dataUpdatedAt?: number;
}
interface ErrorAction<TError> {
    type: 'error';
    error: TError;
}
interface InvalidateAction {
    type: 'invalidate';
}
interface PauseAction {
    type: 'pause';
}
interface ContinueAction {
    type: 'continue';
}
interface SetStateAction<TData, TError> {
    type: 'setState';
    state: QueryState<TData, TError>;
    setStateOptions?: SetStateOptions;
}
export declare type Action<TData, TError> = ContinueAction | ErrorAction<TError> | FailedAction | FetchAction | InvalidateAction | PauseAction | SetStateAction<TData, TError> | SuccessAction<TData>;
export interface SetStateOptions {
    meta?: any;
}
export declare class Query<TQueryFnData = unknown, TError = unknown, TData = TQueryFnData, TQueryKey extends QueryKey = QueryKey> {
    queryKey: TQueryKey;
    queryHash: string;
    options: QueryOptions<TQueryFnData, TError, TData, TQueryKey>;
    initialState: QueryState<TData, TError>;
    revertState?: QueryState<TData, TError>;
    state: QueryState<TData, TError>;
    cacheTime: number;
    meta: QueryMeta | undefined;
    private cache;
    private promise?;
    private gcTimeout?;
    private retryer?;
    private observers;
    private defaultOptions?;
    private abortSignalConsumed;
    private hadObservers;
    constructor(config: QueryConfig<TQueryFnData, TError, TData, TQueryKey>);
    private setOptions;
    setDefaultOptions(options: QueryOptions<TQueryFnData, TError, TData, TQueryKey>): void;
    private scheduleGc;
    private clearGcTimeout;
    private optionalRemove;
    setData(updater: Updater<TData | undefined, TData>, options?: SetDataOptions): TData;
    setState(state: QueryState<TData, TError>, setStateOptions?: SetStateOptions): void;
    cancel(options?: CancelOptions): Promise<void>;
    destroy(): void;
    reset(): void;
    isActive(): boolean;
    isFetching(): boolean;
    isStale(): boolean;
    isStaleByTime(staleTime?: number): boolean;
    onFocus(): void;
    onOnline(): void;
    addObserver(observer: QueryObserver<any, any, any, any, any>): void;
    removeObserver(observer: QueryObserver<any, any, any, any, any>): void;
    getObserversCount(): number;
    invalidate(): void;
    fetch(options?: QueryOptions<TQueryFnData, TError, TData, TQueryKey>, fetchOptions?: FetchOptions): Promise<TData>;
    private dispatch;
    protected getDefaultState(options: QueryOptions<TQueryFnData, TError, TData, TQueryKey>): QueryState<TData, TError>;
    protected reducer(state: QueryState<TData, TError>, action: Action<TData, TError>): QueryState<TData, TError>;
}
export {};
