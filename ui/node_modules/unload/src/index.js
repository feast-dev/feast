import isNode from 'detect-node';
import BrowserMethod from './browser.js';
import NodeMethod from './node.js';

const USE_METHOD = isNode ? NodeMethod : BrowserMethod;
const LISTENERS = new Set();

let startedListening = false;
function startListening() {
    if (startedListening) return;
    startedListening = true;
    USE_METHOD.add(runAll);
}

export function add(fn) {
    startListening();
    if (typeof fn !== 'function')
        throw new Error('Listener is no function');
    LISTENERS.add(fn);

    const addReturn = {
        remove: () => LISTENERS.delete(fn),
        run: () => {
            LISTENERS.delete(fn);
            return fn();
        }
    };
    return addReturn;
}

export function runAll() {
    const promises = [];
    LISTENERS.forEach(function (fn) {
        promises.push(fn());
        LISTENERS.delete(fn);
    });
    return Promise.all(promises);
}

export function removeAll() {
    LISTENERS.clear();
}

export function getSize() {
    return LISTENERS.size;
}

export default {
    add,
    runAll,
    removeAll,
    getSize
};
