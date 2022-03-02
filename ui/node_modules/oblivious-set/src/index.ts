
/**
 * this is a set which automatically forgets
 * a given entry when a new entry is set and the ttl
 * of the old one is over
 */
export class ObliviousSet<T = any> {
    public readonly set = new Set();
    public readonly timeMap = new Map();
    constructor(
        public readonly ttl: number
    ) { }

    has(value: T): boolean {
        return this.set.has(value);
    }

    add(value: T): void {
        this.timeMap.set(value, now());
        this.set.add(value);

        /**
         * When a new value is added,
         * start the cleanup at the next tick
         * to not block the cpu for more important stuff
         * that might happen.
         */
        setTimeout(() => {
            removeTooOldValues(this);
        }, 0);
    }

    clear() {
        this.set.clear();
        this.timeMap.clear();
    }
}


/**
 * Removes all entries from the set
 * where the TTL has expired
 */
export function removeTooOldValues(
    obliviousSet: ObliviousSet
) {
    const olderThen = now() - obliviousSet.ttl;
    const iterator = obliviousSet.set[Symbol.iterator]();

    /**
     * Because we can assume the new values are added at the bottom,
     * we start from the top and stop as soon as we reach a non-too-old value.
     */
    while (true) {
        const value = iterator.next().value;
        if (!value) {
            return; // no more elements
        }
        const time = obliviousSet.timeMap.get(value);
        if (time < olderThen) {
            obliviousSet.timeMap.delete(value);
            obliviousSet.set.delete(value);
        } else {
            // We reached a value that is not old enough
            return;
        }
    }
}

export function now(): number {
    return new Date().getTime();
}


