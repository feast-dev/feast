export interface Logger {
    log: LogFunction;
    warn: LogFunction;
    error: LogFunction;
}
declare type LogFunction = (...args: any[]) => void;
export declare function getLogger(): Logger;
export declare function setLogger(newLogger: Logger): void;
export {};
