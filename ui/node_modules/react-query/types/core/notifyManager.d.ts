declare type NotifyCallback = () => void;
declare type NotifyFunction = (callback: () => void) => void;
declare type BatchNotifyFunction = (callback: () => void) => void;
export declare class NotifyManager {
    private queue;
    private transactions;
    private notifyFn;
    private batchNotifyFn;
    constructor();
    batch<T>(callback: () => T): T;
    schedule(callback: NotifyCallback): void;
    /**
     * All calls to the wrapped function will be batched.
     */
    batchCalls<T extends Function>(callback: T): T;
    flush(): void;
    /**
     * Use this method to set a custom notify function.
     * This can be used to for example wrap notifications with `React.act` while running tests.
     */
    setNotifyFunction(fn: NotifyFunction): void;
    /**
     * Use this method to set a custom function to batch notifications together into a single tick.
     * By default React Query will use the batch function provided by ReactDOM or React Native.
     */
    setBatchNotifyFunction(fn: BatchNotifyFunction): void;
}
export declare const notifyManager: NotifyManager;
export {};
