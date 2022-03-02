/**
 * this method uses indexeddb to store the messages
 * There is currently no observerAPI for idb
 * @link https://github.com/w3c/IndexedDB/issues/51
 */

import {
    sleep,
    randomInt,
    randomToken,
    microSeconds as micro,
    isNode
} from '../util.js';

export const microSeconds = micro;
import { ObliviousSet } from 'oblivious-set';

import {
    fillOptionsWithDefaults
} from '../options';

const DB_PREFIX = 'pubkey.broadcast-channel-0-';
const OBJECT_STORE_ID = 'messages';

export const type = 'idb';

export function getIdb() {
    if (typeof indexedDB !== 'undefined') return indexedDB;
    if (typeof window !== 'undefined') {
        if (typeof window.mozIndexedDB !== 'undefined') return window.mozIndexedDB;
        if (typeof window.webkitIndexedDB !== 'undefined') return window.webkitIndexedDB;
        if (typeof window.msIndexedDB !== 'undefined') return window.msIndexedDB;
    }

    return false;
}

export function createDatabase(channelName) {
    const IndexedDB = getIdb();

    // create table
    const dbName = DB_PREFIX + channelName;
    const openRequest = IndexedDB.open(dbName, 1);

    openRequest.onupgradeneeded = ev => {
        const db = ev.target.result;
        db.createObjectStore(OBJECT_STORE_ID, {
            keyPath: 'id',
            autoIncrement: true
        });
    };
    const dbPromise = new Promise((res, rej) => {
        openRequest.onerror = ev => rej(ev);
        openRequest.onsuccess = () => {
            res(openRequest.result);
        };
    });

    return dbPromise;
}

/**
 * writes the new message to the database
 * so other readers can find it
 */
export function writeMessage(db, readerUuid, messageJson) {
    const time = new Date().getTime();
    const writeObject = {
        uuid: readerUuid,
        time,
        data: messageJson
    };

    const transaction = db.transaction([OBJECT_STORE_ID], 'readwrite');

    return new Promise((res, rej) => {
        transaction.oncomplete = () => res();
        transaction.onerror = ev => rej(ev);

        const objectStore = transaction.objectStore(OBJECT_STORE_ID);
        objectStore.add(writeObject);
    });
}

export function getAllMessages(db) {
    const objectStore = db.transaction(OBJECT_STORE_ID).objectStore(OBJECT_STORE_ID);
    const ret = [];
    return new Promise(res => {
        objectStore.openCursor().onsuccess = ev => {
            const cursor = ev.target.result;
            if (cursor) {
                ret.push(cursor.value);
                //alert("Name for SSN " + cursor.key + " is " + cursor.value.name);
                cursor.continue();
            } else {
                res(ret);
            }
        };
    });
}

export function getMessagesHigherThan(db, lastCursorId) {
    const objectStore = db.transaction(OBJECT_STORE_ID).objectStore(OBJECT_STORE_ID);
    const ret = [];

    function openCursor() {
        // Occasionally Safari will fail on IDBKeyRange.bound, this
        // catches that error, having it open the cursor to the first
        // item. When it gets data it will advance to the desired key.
        try {
            const keyRangeValue = IDBKeyRange.bound(lastCursorId + 1, Infinity);
            return objectStore.openCursor(keyRangeValue);
        } catch (e) {
            return objectStore.openCursor();
        }
    }

    return new Promise(res => {
        openCursor().onsuccess = ev => {
            const cursor = ev.target.result;
            if (cursor) {
                if (cursor.value.id < lastCursorId + 1) {
                    cursor.continue(lastCursorId + 1);
                } else {
                    ret.push(cursor.value);
                    cursor.continue();
                }
            } else {
                res(ret);
            }
        };
    });
}

export function removeMessageById(db, id) {
    const request = db.transaction([OBJECT_STORE_ID], 'readwrite')
        .objectStore(OBJECT_STORE_ID)
        .delete(id);
    return new Promise(res => {
        request.onsuccess = () => res();
    });
}

export function getOldMessages(db, ttl) {
    const olderThen = new Date().getTime() - ttl;
    const objectStore = db.transaction(OBJECT_STORE_ID).objectStore(OBJECT_STORE_ID);
    const ret = [];
    return new Promise(res => {
        objectStore.openCursor().onsuccess = ev => {
            const cursor = ev.target.result;
            if (cursor) {
                const msgObk = cursor.value;
                if (msgObk.time < olderThen) {
                    ret.push(msgObk);
                    //alert("Name for SSN " + cursor.key + " is " + cursor.value.name);
                    cursor.continue();
                } else {
                    // no more old messages,
                    res(ret);
                    return;
                }
            } else {
                res(ret);
            }
        };
    });
}

export function cleanOldMessages(db, ttl) {
    return getOldMessages(db, ttl)
        .then(tooOld => {
            return Promise.all(
                tooOld.map(msgObj => removeMessageById(db, msgObj.id))
            );
        });
}

export function create(channelName, options) {
    options = fillOptionsWithDefaults(options);

    return createDatabase(channelName).then(db => {
        const state = {
            closed: false,
            lastCursorId: 0,
            channelName,
            options,
            uuid: randomToken(),
            /**
             * emittedMessagesIds
             * contains all messages that have been emitted before
             * @type {ObliviousSet}
             */
            eMIs: new ObliviousSet(options.idb.ttl * 2),
            // ensures we do not read messages in parrallel
            writeBlockPromise: Promise.resolve(),
            messagesCallback: null,
            readQueuePromises: [],
            db
        };

        /**
         * Handle abrupt closes that do not originate from db.close().
         * This could happen, for example, if the underlying storage is
         * removed or if the user clears the database in the browser's
         * history preferences.
         */
        db.onclose = function () {
            state.closed = true;

            if (options.idb.onclose) options.idb.onclose();
        };

        /**
         * if service-workers are used,
         * we have no 'storage'-event if they post a message,
         * therefore we also have to set an interval
         */
        _readLoop(state);

        return state;
    });
}

function _readLoop(state) {
    if (state.closed) return;

    readNewMessages(state)
        .then(() => sleep(state.options.idb.fallbackInterval))
        .then(() => _readLoop(state));
}


function _filterMessage(msgObj, state) {
    if (msgObj.uuid === state.uuid) return false; // send by own
    if (state.eMIs.has(msgObj.id)) return false; // already emitted
    if (msgObj.data.time < state.messagesCallbackTime) return false; // older then onMessageCallback
    return true;
}

/**
 * reads all new messages from the database and emits them
 */
function readNewMessages(state) {

    // channel already closed
    if (state.closed) return Promise.resolve();

    // if no one is listening, we do not need to scan for new messages
    if (!state.messagesCallback) return Promise.resolve();

    return getMessagesHigherThan(state.db, state.lastCursorId)
        .then(newerMessages => {
            const useMessages = newerMessages
                /**
                 * there is a bug in iOS where the msgObj can be undefined some times
                 * so we filter them out
                 * @link https://github.com/pubkey/broadcast-channel/issues/19
                 */
                .filter(msgObj => !!msgObj)
                .map(msgObj => {
                    if (msgObj.id > state.lastCursorId) {
                        state.lastCursorId = msgObj.id;
                    }
                    return msgObj;
                })
                .filter(msgObj => _filterMessage(msgObj, state))
                .sort((msgObjA, msgObjB) => msgObjA.time - msgObjB.time); // sort by time
            useMessages.forEach(msgObj => {
                if (state.messagesCallback) {
                    state.eMIs.add(msgObj.id);
                    state.messagesCallback(msgObj.data);
                }
            });

            return Promise.resolve();
        });
}

export function close(channelState) {
    channelState.closed = true;
    channelState.db.close();
}

export function postMessage(channelState, messageJson) {

    channelState.writeBlockPromise = channelState.writeBlockPromise
        .then(() => writeMessage(
            channelState.db,
            channelState.uuid,
            messageJson
        ))
        .then(() => {
            if (randomInt(0, 10) === 0) {
                /* await (do not await) */
                cleanOldMessages(
                    channelState.db,
                    channelState.options.idb.ttl
                );
            }
        });

    return channelState.writeBlockPromise;
}

export function onMessage(channelState, fn, time) {
    channelState.messagesCallbackTime = time;
    channelState.messagesCallback = fn;
    readNewMessages(channelState);
}

export function canBeUsed() {
    if (isNode) return false;
    const idb = getIdb();

    if (!idb) return false;
    return true;
}

export function averageResponseTime(options) {
    return options.idb.fallbackInterval * 2;
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
