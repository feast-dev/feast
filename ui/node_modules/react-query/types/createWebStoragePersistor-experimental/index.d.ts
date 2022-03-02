import { PersistedClient, Persistor } from '../persistQueryClient-experimental';
interface CreateWebStoragePersistorOptions {
    /** The storage client used for setting an retrieving items from cache */
    storage: Storage;
    /** The key to use when storing the cache */
    key?: string;
    /** To avoid spamming,
     * pass a time in ms to throttle saving the cache to disk */
    throttleTime?: number;
    /**
     * How to serialize the data to storage.
     * @default `JSON.stringify`
     */
    serialize?: (client: PersistedClient) => string;
    /**
     * How to deserialize the data from storage.
     * @default `JSON.parse`
     */
    deserialize?: (cachedString: string) => PersistedClient;
}
export declare function createWebStoragePersistor({ storage, key, throttleTime, serialize, deserialize, }: CreateWebStoragePersistorOptions): Persistor;
export {};
