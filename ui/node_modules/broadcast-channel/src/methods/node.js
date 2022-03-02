/**
 * this method is used in nodejs-environments.
 * The ipc is handled via sockets and file-writes to the tmp-folder
 */

const util = require('util');
const fs = require('fs');
const os = require('os');
const events = require('events');
const net = require('net');
const path = require('path');
const micro = require('nano-time');
const rimraf = require('rimraf');
const sha3_224 = require('js-sha3').sha3_224;
const isNode = require('detect-node');
const unload = require('unload');

const fillOptionsWithDefaults = require('../../dist/lib/options.js').fillOptionsWithDefaults;
const ownUtil = require('../../dist/lib/util.js');
const randomInt = ownUtil.randomInt;
const randomToken = ownUtil.randomToken;
const { ObliviousSet } = require('oblivious-set');

/**
 * windows sucks, so we have handle windows-type of socket-paths
 * @link https://gist.github.com/domenic/2790533#gistcomment-331356
 */
function cleanPipeName(str) {
    if (
        process.platform === 'win32' &&
        !str.startsWith('\\\\.\\pipe\\')
    ) {
        str = str.replace(/^\//, '');
        str = str.replace(/\//g, '-');
        return '\\\\.\\pipe\\' + str;
    } else {
        return str;
    }
}

const mkdir = util.promisify(fs.mkdir);
const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);
const unlink = util.promisify(fs.unlink);
const readdir = util.promisify(fs.readdir);
const chmod = util.promisify(fs.chmod);
const removeDir = util.promisify(rimraf);

const OTHER_INSTANCES = {};
const TMP_FOLDER_NAME = 'pubkey.bc';
const TMP_FOLDER_BASE = path.join(
    os.tmpdir(),
    TMP_FOLDER_NAME
);
const getPathsCache = new Map();

function getPaths(channelName) {
    if (!getPathsCache.has(channelName)) {
        const channelHash = sha3_224(channelName); // use hash incase of strange characters
        /**
         * because the lenght of socket-paths is limited, we use only the first 20 chars
         * and also start with A to ensure we do not start with a number
         * @link https://serverfault.com/questions/641347/check-if-a-path-exceeds-maximum-for-unix-domain-socket
         */
        const channelFolder = 'A' + channelHash.substring(0, 20);

        const channelPathBase = path.join(
            TMP_FOLDER_BASE,
            channelFolder
        );
        const folderPathReaders = path.join(
            channelPathBase,
            'rdrs'
        );
        const folderPathMessages = path.join(
            channelPathBase,
            'messages'
        );

        const ret = {
            channelBase: channelPathBase,
            readers: folderPathReaders,
            messages: folderPathMessages
        };
        getPathsCache.set(channelName, ret);
        return ret;
    }
    return getPathsCache.get(channelName);
}

let ENSURE_BASE_FOLDER_EXISTS_PROMISE = null;
async function ensureBaseFolderExists() {
    if (!ENSURE_BASE_FOLDER_EXISTS_PROMISE) {
        ENSURE_BASE_FOLDER_EXISTS_PROMISE = mkdir(TMP_FOLDER_BASE).catch(() => null);
    }
    return ENSURE_BASE_FOLDER_EXISTS_PROMISE;
}

async function ensureFoldersExist(channelName, paths) {
    paths = paths || getPaths(channelName);

    await ensureBaseFolderExists();

    await mkdir(paths.channelBase).catch(() => null);
    await Promise.all([
        mkdir(paths.readers).catch(() => null),
        mkdir(paths.messages).catch(() => null)
    ]);

    // set permissions so other users can use the same channel
    const chmodValue = '777';
    await Promise.all([
        chmod(paths.channelBase, chmodValue),
        chmod(paths.readers, chmodValue),
        chmod(paths.messages, chmodValue)
    ]).catch(() => null);
}

/**
 * removes the tmp-folder
 * @return {Promise<true>}
 */
async function clearNodeFolder() {
    if (!TMP_FOLDER_BASE || TMP_FOLDER_BASE === '' || TMP_FOLDER_BASE === '/') {
        throw new Error('BroadcastChannel.clearNodeFolder(): path is wrong');
    }
    ENSURE_BASE_FOLDER_EXISTS_PROMISE = null;
    await removeDir(TMP_FOLDER_BASE);
    ENSURE_BASE_FOLDER_EXISTS_PROMISE = null;
    return true;
}


function socketPath(channelName, readerUuid, paths) {
    paths = paths || getPaths(channelName);
    const socketPath = path.join(
        paths.readers,
        readerUuid + '.s'
    );
    return cleanPipeName(socketPath);
}

function socketInfoPath(channelName, readerUuid, paths) {
    paths = paths || getPaths(channelName);
    const socketPath = path.join(
        paths.readers,
        readerUuid + '.json'
    );
    return socketPath;
}


/**
 * Because it is not possible to get all socket-files in a folder,
 * when used under fucking windows,
 * we have to set a normal file so other readers know our socket exists
 */
function createSocketInfoFile(channelName, readerUuid, paths) {
    const pathToFile = socketInfoPath(channelName, readerUuid, paths);
    return writeFile(
        pathToFile,
        JSON.stringify({
            time: microSeconds()
        })
    ).then(() => pathToFile);
}

/**
 * returns the amount of channel-folders in the tmp-directory
 * @return {Promise<number>}
 */
async function countChannelFolders() {
    await ensureBaseFolderExists();
    const folders = await readdir(TMP_FOLDER_BASE);
    return folders.length;
}


async function connectionError(originalError) {
    const count = await countChannelFolders();

    // we only show the augmented message if there are more then 30 channels
    // because we then assume that BroadcastChannel is used in unit-tests
    if (count < 30) return originalError;

    const addObj = {};
    Object.entries(originalError).forEach(([k, v]) => addObj[k] = v);
    const text = 'BroadcastChannel.create(): error: ' +
        'This might happen if you have created to many channels, ' +
        'like when you use BroadcastChannel in unit-tests.' +
        'Try using BroadcastChannel.clearNodeFolder() to clear the tmp-folder before each test.' +
        'See https://github.com/pubkey/broadcast-channel#clear-tmp-folder';
    const newError = new Error(text + ': ' + JSON.stringify(addObj, null, 2));
    return newError;
}

/**
 * creates the socket-file and subscribes to it
 * @return {{emitter: EventEmitter, server: any}}
 */
async function createSocketEventEmitter(channelName, readerUuid, paths) {
    const pathToSocket = socketPath(channelName, readerUuid, paths);

    const emitter = new events.EventEmitter();
    const server = net
        .createServer(stream => {
            stream.on('end', function () { });
            stream.on('data', function (msg) {
                emitter.emit('data', msg.toString());
            });
        });

    await new Promise((resolve, reject) => {
        server.on('error', async (err) => {
            const useErr = await connectionError(err);
            reject(useErr);
        });

        server.listen(pathToSocket, async (err, res) => {
            if (err) {
                const useErr = await connectionError(err);
                reject(useErr);
            } else resolve(res);
        });
    });

    return {
        path: pathToSocket,
        emitter,
        server
    };
}

async function openClientConnection(channelName, readerUuid) {
    const pathToSocket = socketPath(channelName, readerUuid);
    const client = new net.Socket();
    return new Promise((res, rej) => {
        client.connect(
            pathToSocket,
            () => res(client)
        );
        client.on('error', err => rej(err));
    });
}


/**
 * writes the new message to the file-system
 * so other readers can find it
 * @return {Promise}
 */
function writeMessage(channelName, readerUuid, messageJson, paths) {
    paths = paths || getPaths(channelName);
    const time = microSeconds();
    const writeObject = {
        uuid: readerUuid,
        time,
        data: messageJson
    };

    const token = randomToken();
    const fileName = time + '_' + readerUuid + '_' + token + '.json';

    const msgPath = path.join(
        paths.messages,
        fileName
    );

    return writeFile(
        msgPath,
        JSON.stringify(writeObject)
    ).then(() => {
        return {
            time,
            uuid: readerUuid,
            token,
            path: msgPath
        };
    });
}

/**
 * returns the uuids of all readers
 * @return {string[]}
 */
async function getReadersUuids(channelName, paths) {
    paths = paths || getPaths(channelName);
    const readersPath = paths.readers;
    const files = await readdir(readersPath);

    return files
        .map(file => file.split('.'))
        .filter(split => split[1] === 'json') // do not scan .socket-files
        .map(split => split[0]);
}

async function messagePath(channelName, time, token, writerUuid) {
    const fileName = time + '_' + writerUuid + '_' + token + '.json';

    const msgPath = path.join(
        getPaths(channelName).messages,
        fileName
    );
    return msgPath;
}

async function getAllMessages(channelName, paths) {
    paths = paths || getPaths(channelName);
    const messagesPath = paths.messages;
    const files = await readdir(messagesPath);
    return files.map(file => {
        const fileName = file.split('.')[0];
        const split = fileName.split('_');

        return {
            path: path.join(
                messagesPath,
                file
            ),
            time: parseInt(split[0]),
            senderUuid: split[1],
            token: split[2]
        };
    });
}

function getSingleMessage(channelName, msgObj, paths) {
    paths = paths || getPaths(channelName);

    return {
        path: path.join(
            paths.messages,
            msgObj.t + '_' + msgObj.u + '_' + msgObj.to + '.json'
        ),
        time: msgObj.t,
        senderUuid: msgObj.u,
        token: msgObj.to
    };
}


function readMessage(messageObj) {
    return readFile(messageObj.path, 'utf8')
        .then(content => JSON.parse(content));
}

async function cleanOldMessages(messageObjects, ttl) {
    const olderThen = Date.now() - ttl;
    await Promise.all(
        messageObjects
            .filter(obj => (obj.time / 1000) < olderThen)
            .map(obj => unlink(obj.path).catch(() => null))
    );
}



const type = 'node';

/**
 * creates a new channelState
 * @return {Promise<any>}
 */
async function create(channelName, options = {}) {
    options = fillOptionsWithDefaults(options);
    const time = microSeconds();
    const paths = getPaths(channelName);
    const ensureFolderExistsPromise = ensureFoldersExist(channelName, paths);
    const uuid = randomToken();

    const state = {
        time,
        channelName,
        options,
        uuid,
        paths,
        // contains all messages that have been emitted before
        emittedMessagesIds: new ObliviousSet(options.node.ttl * 2),
        messagesCallbackTime: null,
        messagesCallback: null,
        // ensures we do not read messages in parrallel
        writeBlockPromise: Promise.resolve(),
        otherReaderClients: {},
        // ensure if process crashes, everything is cleaned up
        removeUnload: unload.add(() => close(state)),
        closed: false
    };

    if (!OTHER_INSTANCES[channelName]) OTHER_INSTANCES[channelName] = [];
    OTHER_INSTANCES[channelName].push(state);

    await ensureFolderExistsPromise;
    const [
        socketEE,
        infoFilePath
    ] = await Promise.all([
        createSocketEventEmitter(channelName, uuid, paths),
        createSocketInfoFile(channelName, uuid, paths),
        refreshReaderClients(state)
    ]);
    state.socketEE = socketEE;
    state.infoFilePath = infoFilePath;

    // when new message comes in, we read it and emit it
    socketEE.emitter.on('data', data => {

        // if the socket is used fast, it may appear that multiple messages are flushed at once
        // so we have to split them before
        const singleOnes = data.split('|');
        singleOnes
            .filter(single => single !== '')
            .forEach(single => {
                try {
                    const obj = JSON.parse(single);
                    handleMessagePing(state, obj);
                } catch (err) {
                    throw new Error('could not parse data: ' + single);
                }
            });
    });

    return state;
}

function _filterMessage(msgObj, state) {
    if (msgObj.senderUuid === state.uuid) return false; // not send by own
    if (state.emittedMessagesIds.has(msgObj.token)) return false; // not already emitted
    if (!state.messagesCallback) return false; // no listener
    if (msgObj.time < state.messagesCallbackTime) return false; // not older then onMessageCallback
    if (msgObj.time < state.time) return false; // msgObj is older then channel

    state.emittedMessagesIds.add(msgObj.token);
    return true;
}

/**
 * when the socket pings, so that we now new messages came,
 * run this
 */
async function handleMessagePing(state, msgObj) {

    /**
     * when there are no listener, we do nothing
     */
    if (!state.messagesCallback) return;

    let messages;
    if (!msgObj) {
        // get all
        messages = await getAllMessages(state.channelName, state.paths);
    } else {
        // get single message
        messages = [
            getSingleMessage(state.channelName, msgObj, state.paths)
        ];
    }

    const useMessages = messages
        .filter(msgObj => _filterMessage(msgObj, state))
        .sort((msgObjA, msgObjB) => msgObjA.time - msgObjB.time); // sort by time


    // if no listener or message, so not do anything
    if (!useMessages.length || !state.messagesCallback) return;

    // read contents
    await Promise.all(
        useMessages
            .map(
                msgObj => readMessage(msgObj).then(content => msgObj.content = content)
            )
    );

    useMessages.forEach(msgObj => {
        state.emittedMessagesIds.add(msgObj.token);

        if (state.messagesCallback) {
            // emit to subscribers
            state.messagesCallback(msgObj.content.data);
        }
    });
}

/**
 * ensures that the channelState is connected with all other readers
 * @return {Promise<void>}
 */
function refreshReaderClients(channelState) {
    return getReadersUuids(channelState.channelName, channelState.paths)
        .then(otherReaders => {
            // remove subscriptions to closed readers
            Object.keys(channelState.otherReaderClients)
                .filter(readerUuid => !otherReaders.includes(readerUuid))
                .forEach(async (readerUuid) => {
                    try {
                        await channelState.otherReaderClients[readerUuid].destroy();
                    } catch (err) { }
                    delete channelState.otherReaderClients[readerUuid];
                });

            // add new readers
            return Promise.all(
                otherReaders
                    .filter(readerUuid => readerUuid !== channelState.uuid) // not own
                    .filter(readerUuid => !channelState.otherReaderClients[readerUuid]) // not already has client
                    .map(async (readerUuid) => {
                        try {
                            if (channelState.closed) return;
                            try {
                                const client = await openClientConnection(channelState.channelName, readerUuid);
                                channelState.otherReaderClients[readerUuid] = client;
                            } catch (err) {
                                // this can throw when the cleanup of another channel was interrupted
                                // or the socket-file does not exits yet
                            }
                        } catch (err) {
                            // this might throw if the other channel is closed at the same time when this one is running refresh
                            // so we do not throw an error
                        }
                    })
            );
        });
}

/**
 * post a message to the other readers
 * @return {Promise<void>}
 */
function postMessage(channelState, messageJson) {
    const writePromise = writeMessage(
        channelState.channelName,
        channelState.uuid,
        messageJson,
        channelState.paths
    );
    channelState.writeBlockPromise = channelState.writeBlockPromise.then(async () => {

        // w8 one tick to let the buffer flush
        await new Promise(res => setTimeout(res, 0));

        const [msgObj] = await Promise.all([
            writePromise,
            refreshReaderClients(channelState)
        ]);
        emitOverFastPath(channelState, msgObj, messageJson);
        const pingStr = '{"t":' + msgObj.time + ',"u":"' + msgObj.uuid + '","to":"' + msgObj.token + '"}|';

        const writeToReadersPromise = Promise.all(
            Object.values(channelState.otherReaderClients)
                .filter(client => client.writable) // client might have closed in between
                .map(client => {
                    return new Promise(res => {
                        client.write(pingStr, res);
                    });
                })
        );

        /**
         * clean up old messages
         * to not waste resources on cleaning up,
         * only if random-int matches, we clean up old messages
         */
        if (randomInt(0, 20) === 0) {
            /* await */
            getAllMessages(channelState.channelName, channelState.paths)
                .then(allMessages => cleanOldMessages(allMessages, channelState.options.node.ttl));
        }

        return writeToReadersPromise;
    });

    return channelState.writeBlockPromise;
}

/**
 * When multiple BroadcastChannels with the same name
 * are created in a single node-process, we can access them directly and emit messages.
 * This might not happen often in production
 * but will speed up things when this module is used in unit-tests.
 */
function emitOverFastPath(state, msgObj, messageJson) {
    if (!state.options.node.useFastPath) return; // disabled
    const others = OTHER_INSTANCES[state.channelName].filter(s => s !== state);

    const checkObj = {
        time: msgObj.time,
        senderUuid: msgObj.uuid,
        token: msgObj.token
    };

    others
        .filter(otherState => _filterMessage(checkObj, otherState))
        .forEach(otherState => {
            otherState.messagesCallback(messageJson);
        });
}


function onMessage(channelState, fn, time = microSeconds()) {
    channelState.messagesCallbackTime = time;
    channelState.messagesCallback = fn;
    handleMessagePing(channelState);
}

/**
 * closes the channel
 * @return {Promise}
 */
function close(channelState) {
    if (channelState.closed) return;
    channelState.closed = true;
    channelState.emittedMessagesIds.clear();
    OTHER_INSTANCES[channelState.channelName] = OTHER_INSTANCES[channelState.channelName].filter(o => o !== channelState);

    if (channelState.removeUnload) {
        channelState.removeUnload.remove();
    }

    return new Promise((res) => {

        if (channelState.socketEE)
            channelState.socketEE.emitter.removeAllListeners();

        Object.values(channelState.otherReaderClients)
            .forEach(client => client.destroy());

        if (channelState.infoFilePath) {
            try {
                fs.unlinkSync(channelState.infoFilePath);
            } catch (err) { }
        }

        /**
         * the server get closed lazy because others might still write on it
         * and have not found out that the infoFile was deleted
         */
        setTimeout(() => {
            channelState.socketEE.server.close();
            res();
        }, 200);
    });
}


function canBeUsed() {
    return isNode;
}

/**
 * on node we use a relatively height averageResponseTime,
 * because the file-io might be in use.
 * Also it is more important that the leader-election is reliable,
 * then to have a fast election.
 */
function averageResponseTime() {
    return 200;
}

function microSeconds() {
    return parseInt(micro.microseconds());
}

module.exports = {
    TMP_FOLDER_BASE,
    cleanPipeName,
    getPaths,
    ensureFoldersExist,
    clearNodeFolder,
    socketPath,
    socketInfoPath,
    createSocketInfoFile,
    countChannelFolders,
    createSocketEventEmitter,
    openClientConnection,
    writeMessage,
    getReadersUuids,
    messagePath,
    getAllMessages,
    getSingleMessage,
    readMessage,
    cleanOldMessages,
    type,
    create,
    _filterMessage,
    handleMessagePing,
    refreshReaderClients,
    postMessage,
    emitOverFastPath,
    onMessage,
    close,
    canBeUsed,
    averageResponseTime,
    microSeconds
};
