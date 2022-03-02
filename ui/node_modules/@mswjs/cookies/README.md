[![Latest version](https://img.shields.io/npm/v/@mswjs/cookies.svg)](https://www.npmjs.com/package/@mswjs/cookies)

# Cookies

Virtual cookie store for managing request/response cookies relations in a unique collection.

## Install

```bash
$ npm install @mswjs/cookies
```

## API

### `set(req: Request, res: Response)`

Sets the response cookies in the store associated with the given request origin.

```js
store.set(
  new Request('https://mswjs.io'),
  new Response(null, {
    headers: new Headers({
      'set-cookie': 'id=abc-123',
    }),
  }),
)
```

### `get(req: Request)`

Retrieves the cookies relevant to the given request's origin.

```js
store.get(new Request('https://mswjs.io'))
```

> `.get()` respects the `req.credentials` policy.

Executing this command returns a `Map` instance with the request cookies:

```js
Map {
  "id" => { name: "id", value: "abc-123" }
}
```

### `getAll()`

Returns all the cookies in the store.

Executing `.getAll()` method returns a `Map` instance with request cookies grouped by request origin.

```js
Map {
  "https://mswjs.io" => Map {
    "id" => { name: "id", value: "abc-123" }
  }
}
```

### `deleteAll(req: Request)`

Removes all the cookies associated with the given request's origin.

### `persist()`

Persists the current store state in the `localStorage`.

### `hydrate()`

Hydrates the store values from the previously persisted state in `localStorage`.

### `clear()`

Removes all the cookies from the store, producing a nice and shiny empty store.

## Credits

Original idea by [
Christoph Guttandin](https://github.com/chrisguttandin).
