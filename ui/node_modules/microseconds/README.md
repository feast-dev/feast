# μs

Generate and parse microseconds.

Uses [hrtime](https://nodejs.org/api/process.html#process_process_hrtime) in node.js, [performance.now](https://developer.mozilla.org/en-US/docs/Web/API/Performance.now()) in browsers. Falls back to `Date.now() * 1000`.

## API

```js
const μs = require('microseconds')
```

### now

timestamp in microseconds

```js
const now = μs.now()
// 1404398280599786
```

### parse

as an object

```js
const parsed = μs.parse(now)
// { microseconds: 786, milliseconds: 599, seconds: 0, minutes: 38, hours: 14, days: 16254 }
```

as a string

```js
parsed.toString()
// "16254 days 14 hours 38 minutes 0 seconds 599 milliseconds 786 microseconds"

μs.parse(1000).toString()
// "1 millisecond"

μs.parse(1).toString()
// "1 microsecond"

μs.parse(4231002).toString()
// "4 seconds 231 milliseconds 2 microseconds"
```

### since

```js
const before = μs.now()
const time = μs.since(before) // time passed
```
