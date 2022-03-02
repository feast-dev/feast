# Advanced Features

All the advanced features of this loader involve customising the `join` option.

Jump to the **"how to"** section -
  * [How to: change precedence of source locations](#how-to-change-precedence-of-source-locations)
  * [How to: fallback to a theme or other global directory](#how-to-fallback-to-a-theme-or-other-global-directory)
  * [How to: fallback to some other asset file](#how-to-fallback-to-some-other-asset-file)
  * [How to: perform a file-system search for an asset](#how-to-perform-a-file-system-search-for-an-asset)

## What is the "join" function?

The "join" function determines how CSS URIs are combined with one of the possible base paths the algorithm has identified.

⚠️ **IMPORTANT** - First read how the [algorithm](./how-it-works.md#algorithm) works.

The "join" function is a higher-order function created using the `options` and `loader` reference. That gives a function that accepts a single `item` and synchronously returns an absolute asset path to substitute back into the original CSS.

```javascript
(options:{}, loader:{}) =>
  (item:{ uri:string, query: string, isAbsolute: boolean, bases:{} }) =>
    string | null
```

Where the `bases` are absolute directory paths `{ subString, value, property, selector }` per the [algorithm](./how-it-works.md#algorithm). Note that returning `null` implies no substitution, the original relative `uri` is retained.

The job of the "join" function is to consider possible locations for the asset based on the `bases` and determine which is most appropriate. This implies some order of precedence in these locations and some file-system operation to determine if the asset there.

The default implementation is suitable for most users but can be customised per the `join` option.

A custom `join` function from scratch is possible but we've provided some [building blocks](#building-blocks) to make the task easier.

## Building blocks

There are a number of utilities (defined in [`lib/join-function/index.js`](../lib/join-function/index.js)) to help construct a custom "join" function . These are conveniently re-exported as properties of the loader.

These utilities are used to create the `defaultJoin` as follows.

```javascript
const {
  createJoinFunction,
  createJoinImplementation,
  defaultJoinGenerator,
} = require('resolve-url-loader');

// create a join function equivalent to "defaultJoin"
const myJoinFn = createJoinFunction(
  'myJoinFn',
  createJoinImplementation(defaultJoinGenerator),
});
```

🤓 If you have some very specific behaviour in mind you can specify your own implementation. This gives full control but still gives you `debug` logging for free.

```javascript
createJoinFunction = (name:string, implementation: function): function
```

For each item, the implementation needs to make multiple attempts at locating the asset. It has mixed concerns of itentifying locations to search and then evaluating those locates one by one.

👉 However its recommended to instead use `createJoinImplementation` to create the `implementation` using the `generator` concept.

```javascript
createJoinImplementation = (generator: function*): function
```

The `generator` has the single concern of identifying locations to search. The work of searching these locations is done by `createJoinImplementation`. Overall this means less boilerplate code for you to write.

Don't worry, you don't need to use `function*` semantics for the `generator` unless you want to.

## Simple customisation

It is relatively simple to change the precedence of values (from the [algorithm](./how-it-works.md#algorithm)) or add further locations to search for an asset. To do this we use `createJoinImplementation` and write a custom `generator`.

See the reference or jump directly to the [examples](#how-to-change-precedence-of-source-locations).

### Reference

The `generator` identifies `[base:string,uri:string]` tuples describing locations to search for an asset. It does **not** return the final asset path.

You may lazily generate tuples as `Iterator`. Refer to this [guide on Iterators and Generators](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Iterators_and_Generators).
```javascript
generator = function* (item: {}, options: {}, loader: {}): Iterator<[string,string]>
```

Or it can be simpler to write a function that returns `Array` and convert it to a generator using `asGenerator`.

```javascript
generator = asGenerator( function (item: {}, options: {}, loader: {}): Array<string> )
```
```javascript
generator = asGenerator( function (item: {}, options: {}, loader: {}): Array<[string,string]> )
```

When using `asGenerator` you may return elements as either `base:string` **or** `[base:string,uri:string]` tuples.

<details>
<summary>Arguments</summary>

* `item` consist of -
  * `uri: string` is the argument to the `url()` as it appears in the source file.
  * `query: string` is any query or hash string starting with `?` or `#` that suffixes the `uri`
  * `isAbsolute: boolean` flag indicates whether the URI is considered an absolute file or root relative path by webpack's definition. Absolute URIs are only processed if the `root` option is specified.
  * `bases: {}` are a hash where the keys are the sourcemap evaluation locations in the [algorithm](./how-it-works.md#algorithm) and the values are absolute paths that the sourcemap reports. These directories might not actually exist.
* `options` consist of -
   * All documented options for the loader.
   * Any other values you include in the loader configuration for your own purposes.
* `loader` consists of the webpack loader API, useful items include -
   * `fs: {}` the virtual file-system from Webpack.
   * `resourcePath: string` the source file currently being processed.
* returns an `Iterator` with elements of `[base:string,uri:string]` either intrinsically or by using `asGenerator`.
</details>

<details>
<summary>FAQ</summary>

* **Why a tuple?**

  The primary pupose of this loader is to find the correct `base` path for your `uri`. By returning a list of paths to search we can better generate `debug` logging.
  
  That said there are cases where you might want to amend the `uri`. The solution is to make each element a tuple of `base` and `uri` representing a potential location to find the asset.
  
  If you're interested only in the `base` path and don't intend to vary the `uri` then the `asGenerator` utility saves you having to create repetative tuples (and from using `function*` semantics).

* **Can I vary the `query` using the tuple?**

  No. We don't support amending the `query` in the final value. If you would like this enhancement please open an issue.

* **What about duplicate or falsey elements?**

  The `createJoinImplementation` will eliminate any invalid elements regardless of whether you use `Array` or `Iterator`. This makes it possible to `&&` elements inline with a predicate value.
  
  If you use `Array` then `asGenerator` will also remove duplicates.

* **When should I use `function*`?**

  If you need lazy generation of values then you may return `Iterator` or use `function*` semantics. Refer to [this guide on Iterators](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Iterators_and_Generators).
  
  But in most cases, when the values are known apriori, simply returning `Array` has simpler semantics making `asGenerator` preferable.

* **Why is this generator so complicated?**

  The join function must make multiple attempts to join a `base` and `uri` and check that the file exists using webpack `fs`.

  The `generator` is focussed on identifying locations to search. It is a more scalable concept where you wish to search many places. The traditional use case for the custom "join" function is a file-system search so the `generator` was designed to make this possible.

  If you prefer a less abstract approach consider a full `implementation` per the [full customisation](#full-customisation) approach.
</details>

### How to: change precedence of source locations

Source-map sampling is limited to the locations defined in the [algorithm](./how-it-works.md#algorithm). You can't change these locations but you can preference them in a different order.

This example shows the default order which you can easily amend. Absolute URIs are rare in most projects but can be handled for completeness.

**Using `asGenerator`**

```javascript
const {
  createJoinFunction,
  createJoinImplementation,
  asGenerator,
  defaultJoinGenerator,
} = require('resolve-url-loader');

// order source-map sampling location by your preferred precedence (matches defaultJoinGenerator)
const myGenerator = asGenerator(
  ({ isAbsolute, bases: { substring, value, property, selector} }, { root }) =>
    isAbsolute ? [root] : [subString, value, property, selector]
);

const myJoinFn = createJoinFunction(
  'myJoinFn',
  createJoinImplementation(myGenerator),
);
```

**Notes**

* The implementation is the default behaviour, so if you want this precedence do **not** customise the `join` option.
* Absolute URIs generally use the base path given in the `root` option as shown.
* The `asGenerator` utility allows us to return simple `Array<string>` of potential base paths.

### How to: fallback to a theme or other global directory

Additional locations can be added by decorating the default generator. This is popular for adding some sort of "theme" directory containing assets.

This example appends a static theme directory as a fallback location where the asset might reside. Absolute URIs are rare in most projects but can be handled for completeness.

**Using `asGenerator`**

```javascript
const path = require('path');
const {
  createJoinFunction,
  createJoinImplementation,
  asGenerator,
  defaultJoinGenerator,
} = require('resolve-url-loader');

const myThemeDirectory = path.resolve(...);

// call default generator then append any additional paths
const myGenerator = asGenerator(
  (item, ...rest) => [
    ...defaultJoinGenerator(item, ...rest),
    item.isAbsolute ? null : myThemeDirectory,
  ]
);

const myJoinFn = createJoinFunction(
  'myJoinFn',
  createJoinImplementation(myGenerator),
);
```

**Notes**

* By spreading the result of `defaultJoinGenerator` we are first trying the default behaviour. If that is unsuccessful we then try the theme location.
* It's assumed that theming doesn't apply to absolute URIs. Since falsey elements are ignored we can easily `null` the additional theme element inline as shown.
* The `asGenerator` utility allows us to return simple `Array<string>` of potential base paths.

### How to: fallback to some other asset file

Lets imagine we don't have high quality files for all our assets and must sometimes use a lower quality format. For each item we need to try the `uri` with different file extensions. We can do this by returning tuples of `[base:string,uri:string]`.

In this example we prefer the `.svg` asset we are happy to use any available `.png` or `.jpg` instead.

**Using `asGenerator`**

```javascript
const {
  createJoinFunction,
  createJoinImplementation,
  asGenerator,
  defaultJoinGenerator,
} = require('resolve-url-loader');

// call default generator then pair different variations of uri with each base
const myGenerator = asGenerator(
  (item, ...rest) => {
    const defaultTuples = [...defaultJoinGenerator(item, ...rest)];
    return /\.svg$/.test(item.uri)
      ? ['.svg', '.png', 'jpg'].flatMap((ext) =>
          defaultTuples.flatMap(([base, uri]) =>
            [base, uri.replace(/\.svg$/, ext)]
          })
        )
      : defaultTuples;
  }
);

const myJoinFn = createJoinFunction(
  'myJoinFn',
  createJoinImplementation(myGenerator),
);
```

**Using `function*`**

```javascript
const {
  createJoinFunction,
  createJoinImplementation,
  defaultJoinGenerator,
} = require('resolve-url-loader');

// call default generator then pair different variations of uri with each base
const myGenerator = function* (item, ...rest) {
  if (/\.svg$/.test(item.uri)) {
    for (let ext of ['.svg', '.png', 'jpg']) {
      for (let [base, uri] of defaultJoinGenerator(item, ...rest)) {
        yield [base, uri.replace(/\.svg$/, ext)];
      }
    }
  } else {
    for (let value of defaultJoinGenerator(item, ...rest)) {
      yield value;
    }
  }
}

const myJoinFn = createJoinFunction(
  'myJoinFn',
  createJoinImplementation(myGenerator),
);
```

**Notes**

* Existing generators such as `defaultJoinGenerator` will always return `[string,string]` tuples so we can destruture `base` and `uri` values with confidence.
* This implementation attempts all extensions for a given `base` before moving to the next `base`. Obviously we may change the nesting and instead do the oposite, attempt all bases for a single extension before moving on to the next extension
* The `asGenerator` utility allows us to return `Array<[string, string]>` but is **not** needed when we use `function*` semantics.

### How to: perform a file-system search for an asset

⚠️ **IMPORTANT** - This example is indicative only and is **not** advised.

When this loader was originally released it was very common for packages be broken to the point that a full file search was needed to locate assets referred to in CSS. While this was not performant some users really liked it. By customising the `generator` we can once again lazily search the file-system.

In this example we search the parent directories of the base paths, continuing upwards until we hit a package boundary. Absolute URIs are rare in most projects but can be handled for completeness.

**Using `function*`**

```javascript
const path = require('path');
const {
  createJoinFunction,
  createJoinImplementation,
  webpackExistsSync
} = require('resolve-url-loader');

// search up from the initial base path until you hit a package boundary
const myGenerator = function* (
  { uri, isAbsolute, bases: { substring, value, property, selector } },
  { root, attempts = 1e3 },
  { fs },
) {
  if (isAbsolute) {
    yield [root, uri];
  } else {
    for (let base of [subString, value, property, selector]) {
       for (let isDone = false, i = 0; !isDone && i < attempts; i++) {
          yield [base, uri];
          // unfortunately fs.existsSync() is not present so we must shim it
          const maybePkg = path.normalize(path.join(base, 'package.json'));
          try {
            isDone = fs.statSync(maybePkg).isFile();
          } catch (error) {
            isDone = false;
          }
          base = base.split(/(\\\/)/).slice(0, -2).join('');
       }
    }
  }
};

const myJoinFn = createJoinFunction(
  'myJoinFn',
  createJoinImplementation(myGenerator),
);
```

**Notes**

* This implementation is nether tested nor robust, it would need further safeguards to avoid searching the entire file system.

* By using `function*` the generator is lazy. We only walk the file-system directory tree as necessary.

* The webpack file-system is provided by the `enhanced-resolver-plugin` and does **not** contain `fs.existsSync()`. We must use `fs.statsSync()` instead and catch any error where the file isn't present.

* You may set additional `options` when you configure the loader in webpack and then access them in your `generator`. In this case we add an `attempts` option to limit the file search.


## Full customisation

The `createJoinFunction` can give you full control over how the `base` and `uri` are joined to create an absolute file path **and** the definitiion of success for that combination.

It provides additional logging when using `debug` option so is a better choice then writing a "join" function from scratch.

Limited documentation is given here since it is rare to require a full customisation. Refer to the source code for further information.

### Reference

The `implementation` synchronously returns the final asset path or some fallback value. It makes a number of attempts to search for the given item and returns an element describing each attempt.

```javascript
implementation = function (item: {}, options: {}, loader: {}):
  Array<{
    base      : string,
    uri       : string,
    joined    : string,
    isSuccess : boolean,
    isFallback: boolean,
  }>
```
<details>
<summary>Arguments</summary>

* `item` consist of -
  * `uri: string` is the argument to the `url()` as it appears in the source file.
  * `query: string` is any string starting with `?` or `#` that suffixes the `uri`
  * `isAbsolute: boolean` flag indicates whether the URI is considered an absolute file or root relative path by webpack's definition. Absolute URIs are only processed if the `root` option is specified.
  * `bases: {}` are a hash where the keys are the sourcemap evaluation locations in the [algorithm](./how-it-works.md#algorithm) and the values are absolute paths that the sourcemap reports. These directories might not actually exist.
* `options` consist of -
  * All documented options for the loader.
  * Any other values you include in the loader configuration for your own purposes.
* `loader` consists of the webpack loader API, useful items include -
  * `fs: {}` the virtual file-system from Webpack.
  * `resourcePath: string` the source file currently being processed.
* returns an array of attempts that were made in resolving the URI -
  * `base` the base path
  * `uri` the uri path
  * `joined` the absolute path created from the joining the `base` and `uri` paths.
  * `isSuccess` indicates the asset was found and that `joined` should be the final result
  * `isFallback` indicates the asset was not found but that `joined` kis suitable as a fallback value
</details>