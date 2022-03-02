import { QueryFunction, QueryKey } from '../core/types';
import { UseQueryOptions, UseQueryResult } from './types';
export declare function useQuery<TQueryFnData = unknown, TError = unknown, TData = TQueryFnData, TQueryKey extends QueryKey = QueryKey>(options: UseQueryOptions<TQueryFnData, TError, TData, TQueryKey>): UseQueryResult<TData, TError>;
export declare function useQuery<TQueryFnData = unknown, TError = unknown, TData = TQueryFnData, TQueryKey extends QueryKey = QueryKey>(queryKey: TQueryKey, options?: Omit<UseQueryOptions<TQueryFnData, TError, TData, TQueryKey>, 'queryKey'>): UseQueryResult<TData, TError>;
export declare function useQuery<TQueryFnData = unknown, TError = unknown, TData = TQueryFnData, TQueryKey extends QueryKey = QueryKey>(queryKey: TQueryKey, queryFn: QueryFunction<TQueryFnData, TQueryKey>, options?: Omit<UseQueryOptions<TQueryFnData, TError, TData, TQueryKey>, 'queryKey' | 'queryFn'>): UseQueryResult<TData, TError>;
