import { QueryClient } from '../core';
import { DehydratedState, DehydrateOptions, HydrateOptions } from 'react-query';
import { Promisable } from 'type-fest';
export interface Persistor {
    persistClient(persistClient: PersistedClient): Promisable<void>;
    restoreClient(): Promisable<PersistedClient | undefined>;
    removeClient(): Promisable<void>;
}
export interface PersistedClient {
    timestamp: number;
    buster: string;
    clientState: DehydratedState;
}
export interface PersistQueryClientOptions {
    /** The QueryClient to persist */
    queryClient: QueryClient;
    /** The Persistor interface for storing and restoring the cache
     * to/from a persisted location */
    persistor: Persistor;
    /** The max-allowed age of the cache.
     * If a persisted cache is found that is older than this
     * time, it will be discarded */
    maxAge?: number;
    /** A unique string that can be used to forcefully
     * invalidate existing caches if they do not share the same buster string */
    buster?: string;
    /** The options passed to the hydrate function */
    hydrateOptions?: HydrateOptions;
    /** The options passed to the dehydrate function */
    dehydrateOptions?: DehydrateOptions;
}
export declare function persistQueryClient({ queryClient, persistor, maxAge, buster, hydrateOptions, dehydrateOptions, }: PersistQueryClientOptions): Promise<void>;
