# NanoTime

Gets the current time in nanoseconds or microseconds.

In node.js you can get high resolution time with `process.hrtime()`, but it is from an unknown relative time, not epoch. So this library helps that by calculating the difference and adding it to the current time.

## Usage

Since javascript can't hold a nanosecond as an INT safely (_Number.MAX_SAFE_INTEGER_), we return a string instead.

```js
const now = require('nano-time');

now(); // '1476742925219947761' (returns a string)
now.micro(); // '1476742921398373'
now.microseconds(); // alias for now.micro();
```