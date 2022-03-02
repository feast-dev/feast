import { QueryBehavior } from './query';
import { InfiniteData, QueryOptions } from './types';
export declare function infiniteQueryBehavior<TQueryFnData, TError, TData>(): QueryBehavior<TQueryFnData, TError, InfiniteData<TData>>;
export declare function getNextPageParam(options: QueryOptions<any, any>, pages: unknown[]): unknown | undefined;
export declare function getPreviousPageParam(options: QueryOptions<any, any>, pages: unknown[]): unknown | undefined;
/**
 * Checks if there is a next page.
 * Returns `undefined` if it cannot be determined.
 */
export declare function hasNextPage(options: QueryOptions<any, any>, pages?: unknown): boolean | undefined;
/**
 * Checks if there is a previous page.
 * Returns `undefined` if it cannot be determined.
 */
export declare function hasPreviousPage(options: QueryOptions<any, any>, pages?: unknown): boolean | undefined;
