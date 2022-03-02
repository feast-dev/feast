
<p align="center">
  <a href="https://github.com/pubkey/broadcast-channel">
    <img src="../docs/files/icon.png" width="150px" />
  </a>
</p>

<h1 align="center">BroadcastChannel</h1>
<p align="center">
  <strong>A BroadcastChannel to send data between different browser-tabs or nodejs-processes</strong>
  <br/>
  <span>+ LeaderElection over the channels</span><br />
</p>

<p align="center">
    <a href="https://twitter.com/pubkeypubkey">
        <img src="https://img.shields.io/twitter/follow/pubkeypubkey.svg?style=social&logo=twitter"
            alt="follow on Twitter"></a>
</p>

![demo.gif](../docs/files/demo.gif)

* * *

A BroadcastChannel that allows you to send data between different browser-tabs or nodejs-processes.

- It works completely **client-side** and **offline**.
- Tested on **old browsers**, **new browsers**, **WebWorkers**, **Iframes** and **NodeJs**

This behaves similar to the [BroadcastChannel-API](https://developer.mozilla.org/en-US/docs/Web/API/Broadcast_Channel_API) which is currently only featured in [some browsers](https://caniuse.com/#feat=broadcastchannel).

## Using the BroadcastChannel

```bash
npm install --save broadcast-channel
```

#### Create a channel in one tab/process and send a message.

```ts
import { BroadcastChannel } from 'broadcast-channel';
const channel = new BroadcastChannel('foobar');
channel.postMessage('I am not alone');
```

#### Create a channel with the same name in another tab/process and recieve messages.

```ts
import { BroadcastChannel } from 'broadcast-channel';
const channel = new BroadcastChannel('foobar');
channel.onmessage = msg => console.dir(msg);
// > 'I am not alone'
```


#### Add and remove multiple eventlisteners

```ts
import { BroadcastChannel } from 'broadcast-channel';
const channel = new BroadcastChannel('foobar');

const handler = msg => console.log(msg);
channel.addEventListener('message', handler);

// remove it
channel.removeEventListener('message', handler);
```

#### Close the channel if you do not need it anymore.
Returns a `Promise` that resolved when everything is processed.

```js
await channel.close();
```

#### Set options when creating a channel (optional):

```js
const options = {
    type: 'localstorage', // (optional) enforce a type, oneOf['native', 'idb', 'localstorage', 'node']
    webWorkerSupport: true; // (optional) set this to false if you know that your channel will never be used in a WebWorker (increases performance)
};
const channel = new BroadcastChannel('foobar', options);
```

#### Create a typed channel in typescript:

```typescript
import { BroadcastChannel } from 'broadcast-channel';
declare type Message = {
  foo: string;
};
const channel: BroadcastChannel<Message> = new BroadcastChannel('foobar');
channel.postMessage({
  foo: 'bar'
});
```

#### Enforce a options globally

When you use this module in a test-suite, it is recommended to enforce the fast `simulate` method on all channels so your tests run faster. You can do this with `enforceOptions()`. If you set this, all channels have the enforced options, no mather what options are given in the constructor.

```typescript
import { enforceOptions } from 'broadcast-channel';

// enforce this config for all channels
enforceOptions({
  type: 'simulate'
});

// reset the enforcement
enforceOptions(null);
```


#### Clear tmp-folder:
When used in NodeJs, the BroadcastChannel will communicate with other processes over filesystem based sockets.
When you create a huge amount of channels, like you would do when running unit tests, you might get problems because there are too many folders in the tmp-directory. Calling `BroadcastChannel.clearNodeFolder()` will clear the tmp-folder and it is recommended to run this at the beginning of your test-suite.

```typescript
import { clearNodeFolder } from 'broadcast-channel';
// jest
beforeAll(async () => {
  const hasRun = await clearNodeFolder();
  console.log(hasRun); // > true on NodeJs, false on Browsers
})
```

```typescript
import { clearNodeFolder } from 'broadcast-channel';
// mocha
before(async () => {
  const hasRun = await clearNodeFolder();
  console.log(hasRun); // > true on NodeJs, false on Browsers
})
```

#### Handling IndexedDB onclose events

IndexedDB databases can close unexpectedly for various reasons. This could happen, for example, if the underlying storage is removed or if the user clears the database in the browser's history preferences. Most often we have seen this happen in Mobile Safari. By default, we let the connection close and stop polling for changes. If you would like to continue listening you should close BroadcastChannel and create a new one.

Example of how you might do this:

```typescript
import { BroadcastChannel } from 'broadcast-channel';

let channel;

const createChannel = () => {
  channel = new BroadcastChannel(CHANNEL_NAME, {
    idb: {
      onclose: () => {
        // the onclose event is just the IndexedDB closing.
        // you should also close the channel before creating
        // a new one.
        channel.close();
        createChannel();
      },
    },
  });

  channel.onmessage = message => {
    // handle message
  };
};
```

## Methods:

Depending in which environment this is used, a proper method is automatically selected to ensure it always works.

| Method           | Used in                                                         | Description                                                                                                                                             |
| ---------------- | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Native**       | [Modern Browsers](https://caniuse.com/broadcastchannel)         | If the browser supports the BroadcastChannel-API, this method will be used because it is the fastest                                                    |
| **IndexedDB**    | [Browsers with WebWorkers](https://caniuse.com/#feat=indexeddb) | If there is no native BroadcastChannel support, the IndexedDB method is used because it supports messaging between browser-tabs, iframes and WebWorkers |
| **LocalStorage** | [Older Browsers](https://caniuse.com/#feat=namevalue-storage)   | In older browsers that do not support IndexedDb, a localstorage-method is used                                                                          |
| **Sockets**      | NodeJs                                                          | In NodeJs the communication is handled by sockets that send each other messages                                                                         |
| **Simulate**      | none per default                                                          | This method simulates the behavior of the other methods but only runs in the current process without sharing data between processes. Use this method in your test-suite because it is much faster.                                                                  |



## Using the LeaderElection

This module also comes with a leader-election which can be used so elect a leader between different BroadcastChannels.
For example if you have a stable connection from the frontend to your server, you can use the LeaderElection to save server-side performance by only connecting once, even if the user has opened your website in multiple tabs.

In this example the leader is marked with the crown ♛:
![leader-election.gif](../docs/files/leader-election.gif)


Create a channel and an elector.

```ts
import {
  BroadcastChannel,
  createLeaderElection
} from 'broadcast-channel';
const channel = new BroadcastChannel('foobar');
const elector = createLeaderElection(channel);
```

Wait until the elector becomes leader.

```js
import { createLeaderElection } from 'broadcast-channel';
const elector = createLeaderElection(channel);
elector.awaitLeadership().then(()=> {
  console.log('this tab is now leader');
})
```

If more than one tab is becoming leader adjust `LeaderElectionOptions` configuration.

```js
import { createLeaderElection } from 'broadcast-channel';
const elector = createLeaderElection(channel, {
  fallbackInterval: 2000, // optional configuration for how often will renegotiation for leader occur
  responseTime: 1000, // optional configuration for how long will instances have to respond
});
elector.awaitLeadership().then(()=> {
  console.log('this tab is now leader');
})
```

Let the leader die. (automatically happens if the tab is closed or the process exits).

```js
const elector = createLeaderElection(channel);
await elector.die();
```

Handle duplicate leaders. This can happen on rare occurences like when the [CPU is on 100%](https://github.com/pubkey/broadcast-channel/issues/385) for longer time, or the browser [has throttled the javascript timers](https://github.com/pubkey/broadcast-channel/issues/414).

```js
const elector = createLeaderElection(channel);
elector.onduplicate = () => {
  alert('have duplicate leaders!');
}
```


## What this is

This module is optimised for:

- **low latency**: When you postMessage on one channel, it should take as low as possible time until other channels recieve the message.
- **lossless**: When you send a message, it should be impossible that the message is lost before other channels recieved it
- **low idle workload**: During the time when no messages are send, there should be a low processor footprint.

## What this is not

-   This is not a polyfill. Do not set this module to `window.BroadcastChannel`. This implementation behaves similiar to the [BroadcastChannel-Standard](https://developer.mozilla.org/en-US/docs/Web/API/Broadcast_Channel_API) with these limitations:
    - You can only send data that can be `JSON.stringify`-ed.
    - While the offical API emits [onmessage-events](https://developer.mozilla.org/en-US/docs/Web/API/BroadcastChannel/onmessage), this module directly emitts the data which was posted
-   This is not a replacement for a message queue. If you use this in NodeJs and want send more than 50 messages per second, you should use proper [IPC-Tooling](https://en.wikipedia.org/wiki/Message_queue)


## Browser Support
I have tested this in all browsers that I could find. For ie8 and ie9 you must transpile the code before you can use this. If you want to know if this works with your browser, [open the demo page](https://pubkey.github.io/broadcast-channel/e2e.html).

## Thanks
Thanks to [Hemanth.HM](https://github.com/hemanth) for the module name.
