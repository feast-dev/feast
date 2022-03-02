declare type addReturn = {
    remove: () => void;
    run: () => any;
};


export function add(fn: () => void): addReturn;
export function runAll(): Promise<any>;
export function removeAll(): void;
export function getSize(): number;

declare const _default: {
    add,
    runAll,
    removeAll,
    getSize
}

export default _default;