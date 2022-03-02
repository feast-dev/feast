import { CancelOptions } from './types';
interface RetryerConfig<TData = unknown, TError = unknown> {
    fn: () => TData | Promise<TData>;
    abort?: () => void;
    onError?: (error: TError) => void;
    onSuccess?: (data: TData) => void;
    onFail?: (failureCount: number, error: TError) => void;
    onPause?: () => void;
    onContinue?: () => void;
    retry?: RetryValue<TError>;
    retryDelay?: RetryDelayValue<TError>;
}
export declare type RetryValue<TError> = boolean | number | ShouldRetryFunction<TError>;
declare type ShouldRetryFunction<TError = unknown> = (failureCount: number, error: TError) => boolean;
export declare type RetryDelayValue<TError> = number | RetryDelayFunction<TError>;
declare type RetryDelayFunction<TError = unknown> = (failureCount: number, error: TError) => number;
interface Cancelable {
    cancel(): void;
}
export declare function isCancelable(value: any): value is Cancelable;
export declare class CancelledError {
    revert?: boolean;
    silent?: boolean;
    constructor(options?: CancelOptions);
}
export declare function isCancelledError(value: any): value is CancelledError;
export declare class Retryer<TData = unknown, TError = unknown> {
    cancel: (options?: CancelOptions) => void;
    cancelRetry: () => void;
    continueRetry: () => void;
    continue: () => void;
    failureCount: number;
    isPaused: boolean;
    isResolved: boolean;
    isTransportCancelable: boolean;
    promise: Promise<TData>;
    private abort?;
    constructor(config: RetryerConfig<TData, TError>);
}
export {};
