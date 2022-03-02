[![Build status](https://circleci.com/gh/open-draft/until.svg?style=shield)](https://circleci.com/gh/open-draft/until)

# `until`

Gracefully handle a Promise using `async`/`await`.

## Why?

With the addition of `async`/`await` keywords in ECMAScript 2017 the handling of Promises became much easier. However, one must keep in mind that the `await` keyword provides no standard error handling API. Consider this usage:

```js
function getUser(id) {
  const data = await fetchUser(id)
  // Work with "data"...
}
```

In case `fetchUser()` throws an error, the entire `getUser()` function's scope will terminate. Because of this, it's recommended to implement error handling using `try`/`catch` block wrapping `await` expressions:

```js
function getUser(id)
  let data = null

  try {
    data = await asyncAction()
  } catch (error) {
    console.error(error)
  }

  // Work with "data"...
}
```

While this is a semantically valid approach, constructing `try`/`catch` around each awaited operation may be tedious and get overlooked at times. Such error handling also introduces separate closures for execution and error scenarios of an asynchronous operation.

This library encapsulates the `try`/`catch` error handling in a utility function that does not create a separate closure and exposes a NodeJS-friendly API to work with errors and resolved data.

## Getting started

### Install

```bash
npm install @open-draft/until
```

### Usage

```js
import { until } from '@open-draft/until'

async function(id) {
  const [error, user] = await until(() => fetchUser(id))

  if (error) {
    return handleError(error)
  }

  return user
}
```

### Usage with TypeScript

```ts
import { until } from '@open-draft/until'

interface User {
  firstName: string
  age: number
}

interface UserFetchError {
  type: 'FORBIDDEN' | 'NOT_FOUND'
  message?: string
}

async function(id: string) {
  const [error, user] = await until<User, UserFetchError>(() => fetchUser(id))

  if (error) {
    handleError(error.type, error.message)
  }

  return user.firstName
}
```

## Special thanks

- [giuseppegurgone](https://twitter.com/giuseppegurgone) for discussing the original `until` API
