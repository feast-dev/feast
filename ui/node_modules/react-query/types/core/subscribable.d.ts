declare type Listener = () => void;
export declare class Subscribable<TListener extends Function = Listener> {
    protected listeners: TListener[];
    constructor();
    subscribe(listener?: TListener): () => void;
    hasListeners(): boolean;
    protected onSubscribe(): void;
    protected onUnsubscribe(): void;
}
export {};
