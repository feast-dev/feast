# oblivious-set
Like a JavaScript Set() but with a TTL for entries.

In difference to other caches with TTLs out there, this one does not need intervals or timeouts to work.
This means it can be properly garbage collected when there is no more reference to the instance.


## Usage


```ts

import { ObliviousSet } from 'oblivious-set';

// create a set
const obliviousSet = new ObliviousSet(
    100 // TTL in milliseconds
);

// add a value
obliviousSet.add('foobar');

// check existence
console.log(obliviousSet.has('foobar')); // > true
console.log(obliviousSet.has('barfoo')); // > false


// clear
obliviousSet.clear();
```
