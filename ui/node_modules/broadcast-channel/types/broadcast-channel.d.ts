declare type MethodType = 'node' | 'idb' | 'native' | 'localstorage' | 'simulate';



interface BroadcastChannelEventMap {
    "message": MessageEvent;
    "messageerror": MessageEvent;
}

export interface BroadcastMethod<State = object> {
    type: string;
    microSeconds(): number;
    create(channelName: string, options: BroadcastChannelOptions): Promise<State> | State;
    close(channelState: State): void;
    onMessage(channelState: State, callback: (args: any) => void): void;
    postMessage(channelState: State, message: any): Promise<any>;
    canBeUsed(): boolean;
    averageResponseTime(): number;
}

export type BroadcastChannelOptions = {
    type?: MethodType,
    methods?: BroadcastMethod[] | BroadcastMethod,
    webWorkerSupport?: boolean;
    prepareDelay?: number;
    node?: {
        ttl?: number;
        useFastPath?: boolean;
    };
    idb?: {
        ttl?: number;
        fallbackInterval?: number;
        onclose?: () => void;
    };
};

declare type EventContext = 'message' | 'internal' | 'leader';

declare type OnMessageHandler<T> = ((this: BroadcastChannel, ev: T) => any) | null;

/**
 * api as defined in
 * @link https://html.spec.whatwg.org/multipage/web-messaging.html#broadcasting-to-other-browsing-contexts
 * @link https://github.com/Microsoft/TypeScript/blob/master/src/lib/webworker.generated.d.ts#L325
 */
export class BroadcastChannel<T = any> {
    constructor(name: string, opts?: BroadcastChannelOptions);
    readonly name: string;
    readonly options: BroadcastChannelOptions;
    readonly type: MethodType;
    readonly isClosed: boolean;

    postMessage(msg: T): Promise<void>;
    close(): Promise<void>;

    onmessage: OnMessageHandler<T>;

    // not defined in the offical standard
    addEventListener(type: EventContext, handler: OnMessageHandler<T>): void;
    removeEventListener(type: EventContext, handler: OnMessageHandler<T>): void;

}
// statics
export function clearNodeFolder(opts?: BroadcastChannelOptions): Promise<boolean>;
export function enforceOptions(opts?: BroadcastChannelOptions | false | null): void;
