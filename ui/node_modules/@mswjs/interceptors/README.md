[![Latest version](https://img.shields.io/npm/v/@mswjs/interceptors.svg)](https://www.npmjs.com/package/@mswjs/interceptors)
[![Build status](https://img.shields.io/circleci/project/github/mswjs/interceptors/master.svg)](https://app.circleci.com/pipelines/github/mswjs/interceptors)

# `@mswjs/interceptors`

Low-level HTTP/HTTPS/XHR/fetch request interception library.

**Intercepts any requests issued by:**

- `http.get`/`http.request`
- `https.get`/`https.request`
- `XMLHttpRequest`
- `fetch`
- Any third-party libraries that use the modules above (i.e. `request`, `node-fetch`, etc.)

## Motivation

While there are a lot of network communication mocking libraries, they tend to use request interception as an implementation detail, giving you a high-level API that includes request matching, timeouts, retries, and so forth.

This library is a strip-to-bone implementation that provides as little abstraction as possible to execute arbitrary logic upon any request. It's primarily designed as an underlying component for high-level API mocking solutions such as [Mock Service Worker](https://github.com/mswjs/msw).

### How is this library different?

As interception is often combined with request route matching, some libraries can determine whether a request should be mocked _before_ it actually happens. This approach is not suitable for this library, as it rather _intercepts all requests_ and then let's you decide which ones should be mocked. This affects the level at which interception happens, and also the way mocked/original responses are constructed, in comparison to other solutions.

## What this library does

This library monkey-patches the following native modules:

- `http.get`/`http.request`
- `https.get`/`https.request`
- `XMLHttpRequest`
- `fetch`

Once patched, it provisions the interception of requests and normalizes them to something called _isomorphic request instances_. That normalization ensures the same request handling for the consumer of the library, while requests originating from different modules may differ internally.

In its mocking phase, this library accepts an _isomorphic response instance_ that describes a module-agnostic mocked response. This allows you to respond to requests issued by different modules using the same response instance.

## What this library doesn't do

- Does **not** provide any request matching logic.
- Does **not** decide how to handle requests.

## Getting started

```bash
npm install @mswjs/interceptors
```

## API

### `createInterceptor(options: CreateInterceptorOptions)`

Enables request interception in the current process.

```js
import { createInterceptor } from '@mswjs/interceptors'
import nodeInterceptors from '@mswjs/interceptors/lib/presets/node'

const interceptor = createInterceptor({
  modules: nodeInterceptors,
  resolver(request, ref) {
    // Optionally, return a mocked response.
  },
})
```

> Using the `/presets/node` interceptors preset is the recommended way to ensure all requests get intercepted, regardless of their origin.

### `createRemoteInterceptor(options: CreateRemoteInterceptorOptions)`

Enables request interception in the current process while delegating the response resolution logic to the _parent process_. **Requires the current process to be a child process**. Requires the parent process to establish a resolver by calling the `createRemoteResolver` function.

```js
import { createRemoteInterceptor } from '@mswjs/interceptors'

const interceptor = createRemoteInterceptor({
  modules: nodeInterceptors,
})

interceptor.apply()

process.on('disconnect', () => {
  interceptor.restore()
})
```

### `createRemoteResolver(options: CreateRemoteResolverOptions)`

Resolves an intercepted request in the given child `process`. Requires for that child process to enable request interception by calling the `createRemoteInterceptor` function.

```js
import { spawn } from 'child_process'
import { createRemoteResolver } from '@mswjs/interceptors'

const appProcess = spawn('node', ['app.js'], {
  stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
})

createRemoteResolver({
  process: appProcess,
  resolver(request) {
    // Optionally, return a mocked response
    // for a request that occurred in the "appProcess".
  },
})
```

### Interceptors

This library utilizes a concept of _interceptors_–functions that patch necessary modules, handle mocked responses, and restore patched modules.

**List of interceptors:**

- `/interceptors/ClientRequest`
- `/interceptors/XMLHttpRequest`
- `/interceptors/fetch`

To use a single, or multiple interceptors, import and provide them to the `RequestInterceptor` constructor.

```js
import { createInterceptor } from '@mswjs/interceptors'
import { interceptXMLHttpRequest } from '@mswjs/interceptors/lib/interceptors/XMLHttpRequest'

// This `interceptor` instance would handle only XMLHttpRequest,
// ignoring requests issued via `http`/`https` modules.
const interceptor = new createInterceptor({
  modules: [interceptXMLHttpRequest],
})
```

> Interceptors are crucial in leveraging environment-specific module overrides. Certain environments (i.e. React Native) do not have access to native Node.js modules (like `http`). Importing such modules raises an exception, and must be avoided.

### Methods

#### `.apply(): void`

Applies module patches and enables interception of the requests.

```js
interceptor.apply()
```

#### `.on(event, listener): boolean`

Adds an event listener to one of the following supported events:

- `request`, whenever a new request happens.
- `response`, whenever a request library responds to a request.

```js
interceptor.on('request', (request) => {
  console.log('[%s] %s', request.method, request.url.toString())
})
```

#### `.restore(): void`

Restores all patched modules and stops intercepting future requests.

```js
interceptor.restore()
```

## Special mention

The following libraries were used as an inspiration to write this low-level API:

- [`node`](https://github.com/nodejs/node)
- [`nock`](https://github.com/nock/nock)
- [`mock-xmlhttprequest`](https://github.com/berniegp/mock-xmlhttprequest)
