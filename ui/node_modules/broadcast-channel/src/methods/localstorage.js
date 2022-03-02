/**
 * A localStorage-only method which uses localstorage and its 'storage'-event
 * This does not work inside of webworkers because they have no access to locastorage
 * This is basically implemented to support IE9 or your grandmothers toaster.
 * @link https://caniuse.com/#feat=namevalue-storage
 * @link https://caniuse.com/#feat=indexeddb
 */

import { ObliviousSet } from 'oblivious-set';

import {
    fillOptionsWithDefaults
} from '../options';

import {
    sleep,
    randomToken,
    microSeconds as micro,
    isNode
} from '../util';

export const microSeconds = micro;

const KEY_PREFIX = 'pubkey.broadcastChannel-';
export const type = 'localstorage';

/**
 * copied from crosstab
 * @link https://github.com/tejacques/crosstab/blob/master/src/crosstab.js#L32
 */
export function getLocalStorage() {
    let localStorage;
    if (typeof window === 'undefined') return null;
    try {
        localStorage = window.localStorage;
        localStorage = window['ie8-eventlistener/storage'] || window.localStorage;
    } catch (e) {
        // New versions of Firefox throw a Security exception
        // if cookies are disabled. See
        // https://bugzilla.mozilla.org/show_bug.cgi?id=1028153
    }
    return localStorage;
}

export function storageKey(channelName) {
    return KEY_PREFIX + channelName;
}


/**
* writes the new message to the storage
* and fires the storage-event so other readers can find it
*/
export function postMessage(channelState, messageJson) {
    return new Promise(res => {
        sleep().then(() => {
            const key = storageKey(channelState.channelName);
            const writeObj = {
                token: randomToken(),
                time: new Date().getTime(),
                data: messageJson,
                uuid: channelState.uuid
            };
            const value = JSON.stringify(writeObj);
            getLocalStorage().setItem(key, value);

            /**
             * StorageEvent does not fire the 'storage' event
             * in the window that changes the state of the local storage.
             * So we fire it manually
             */
            const ev = document.createEvent('Event');
            ev.initEvent('storage', true, true);
            ev.key = key;
            ev.newValue = value;
            window.dispatchEvent(ev);

            res();
        });
    });
}

export function addStorageEventListener(channelName, fn) {
    const key = storageKey(channelName);
    const listener = ev => {
        if (ev.key === key) {
            fn(JSON.parse(ev.newValue));
        }
    };
    window.addEventListener('storage', listener);
    return listener;
}
export function removeStorageEventListener(listener) {
    window.removeEventListener('storage', listener);
}

export function create(channelName, options) {
    options = fillOptionsWithDefaults(options);
    if (!canBeUsed()) {
        throw new Error('BroadcastChannel: localstorage cannot be used');
    }

    const uuid = randomToken();

    /**
     * eMIs
     * contains all messages that have been emitted before
     * @type {ObliviousSet}
     */
    const eMIs = new ObliviousSet(options.localstorage.removeTimeout);

    const state = {
        channelName,
        uuid,
        eMIs // emittedMessagesIds
    };


    state.listener = addStorageEventListener(
        channelName,
        (msgObj) => {
            if (!state.messagesCallback) return; // no listener
            if (msgObj.uuid === uuid) return; // own message
            if (!msgObj.token || eMIs.has(msgObj.token)) return; // already emitted
            if (msgObj.data.time && msgObj.data.time < state.messagesCallbackTime) return; // too old

            eMIs.add(msgObj.token);
            state.messagesCallback(msgObj.data);
        }
    );


    return state;
}

export function close(channelState) {
    removeStorageEventListener(channelState.listener);
}

export function onMessage(channelState, fn, time) {
    channelState.messagesCallbackTime = time;
    channelState.messagesCallback = fn;
}

export function canBeUsed() {
    if (isNode) return false;
    const ls = getLocalStorage();

    if (!ls) return false;

    try {
        const key = '__broadcastchannel_check';
        ls.setItem(key, 'works');
        ls.removeItem(key);
    } catch (e) {
        // Safari 10 in private mode will not allow write access to local
        // storage and fail with a QuotaExceededError. See
        // https://developer.mozilla.org/en-US/docs/Web/API/Web_Storage_API#Private_Browsing_Incognito_modes
        return false;
    }

    return true;
}


export function averageResponseTime() {
    const defaultTime = 120;
    const userAgent = navigator.userAgent.toLowerCase();
    if (userAgent.includes('safari') && !userAgent.includes('chrome')) {
        // safari is much slower so this time is higher
        return defaultTime * 2;
    }
    return defaultTime;
}

export default {
    create,
    close,
    onMessage,
    postMessage,
    canBeUsed,
    type,
    averageResponseTime,
    microSeconds
};
