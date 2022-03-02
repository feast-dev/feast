# set-cookie-parser 

[![NPM version][npm-image]][npm-url] 
[![Build Status][travis-image]][travis-url] 
[![Dependency Status][daviddm-image]][daviddm-url]
[![npm downloads](https://img.shields.io/npm/dm/set-cookie-parser)][npm-url]

Parses set-cookie headers into objects

Accepts a single `set-cookie` header value, an array of `set-cookie` header values, or a Node.js response object that may have 0 or more `set-cookie` headers.

Also accepts an optional options object. Defaults:

```js
{
    decodeValues: true,  // Calls dcodeURIComponent on each value - default: true
    map: false,          // Return an object instead of an array - default: false
    silent: false,       // Suppress the warning that is loged when called on a request instead of a response - default: false
}
```

Returns either array of cookie objects or map of cookie objects based on `map` option. Each object will have, at a minimum a name and value and may have any of the other parameters depending on the set-cookie header:

* name - cookie name (string)
* value - cookie value (string)
* path - cookie path (string or undefined)
* domain - domain for the cookie (string or undefined, may begin with "." to indicate the named domain or any subdomain of it)
* expires - absolute expiration date for the cookie (Date object or undefined)
* maxAge - relative max age of the cookie in seconds from when the client receives it (integer or undefined)
  * Note: when using with [express's res.cookie() method](http://expressjs.com/en/4x/api.html#res.cookie), multiply `maxAge` by 1000 to convert to miliseconds.
* secure - indicates that this cookie should only be sent over HTTPs (true or undefined)
* httpOnly - indicates that this cookie should *not* be accessible to client-side JavaScript (true or undefined)
* sameSite - indicates a cookie ought not to be sent along with cross-site requests (string or undefined)

(The output format is loosely based on the input format of https://www.npmjs.com/package/cookie)

## Install

```sh
$ npm install --save set-cookie-parser
```


## Usage

Get array of cookie objects
```js
var http = require('http');
var setCookie = require('set-cookie-parser');

http.get('http://example.com', function(res) {
  var cookies = setCookie.parse(res, {
    decodeValues: true  // default: true
  });

  cookies.forEach(console.log);
}
```
Get map of cookie objects
```js
var http = require('http');
var setCookie = require('set-cookie-parser');

http.get('http://example.com', function(res) {
  var cookies = setCookie.parse(res, {
    decodeValues: true,  // default: true
    map: true           //default: false
  });

  var desiredCookie = cookies['session'];
  console.log(desiredCookie);
});
```

Example output:

Array of cookie objects
```js
[
    {
        name: 'bam',
        value: 'baz'
    },
    {
        name: 'foo',
        value: 'bar',
        path: '/',
        expires: new Date('Tue Jul 01 2025 06:01:11 GMT-0400 (EDT)'),
        maxAge: 1000,
        domain: '.example.com',
        secure: true,
        httpOnly: true,
        sameSite: 'lax'
    }
]
```

Map of cookie objects
```js
{
    bam: {
        name: 'bam',
        value: 'baz'
    },
    foo: {
        name: 'foo',
        value: 'bar',
        path: '/',
        expires: new Date('Tue Jul 01 2025 06:01:11 GMT-0400 (EDT)'),
        maxAge: 1000,
        domain: '.example.com',
        secure: true,
        httpOnly: true,
        sameSite: 'lax'
    }
}
```

### Creating a new, modified set-cookie header

This library can be used in conjunction with the [cookie](https://www.npmjs.com/package/cookie) library to modify and replace set-cookie headers:

```js
const libCookie = require('cookie');
const setCookie = require('set-cookie-parser');

function modifySetCookie(res){
  // parse the set-cookie headers with this library
  let cookies = setCookie.parse(res);
  
  // modify the cookies here
  // ...
  
  // create new set-cookie headers using the cookie library
  res.headers['set-cookie'] = cookies.map(function(cookie) {
      return libCookie.serialize(cookie.name, cookie.value, cookie);
  });
}
```

See a real-world example of this in [unblocker](https://github.com/nfriedly/node-unblocker/blob/a894c0861af26489d2467c94fbc500770f57464f/lib/cookies.js#L62-L76)

## Usage in React Native

React Native follows the Fetch spec more closely and combines all of the Set-Cookie header values into a single string. 
The `splitCookiesString` method reverses this.

```js
var setCookie = require('set-cookie-parser');

var response = fetch(/*...*/);

// This is mainly for React Native; Node.js does not combine set-cookie headers.
var combinedCookieHeader = response.headers.get('Set-Cookie');
var splitCookieHeaders = setCookie.splitCookiesString(combinedCookieHeader)
var cookies = setCookie.parse(splitCookieHeaders);

console.log(cookies); // should be an array of cookies
```

This behavior may become a default part of parse in the next major release, but requires the extra step for now.

## API

### parse(input, [options])

Parses cookies from a string, array of strings, or a http response object. 
Always returns an array, regardless of input format. (Unless the `map` option is set, in which case it always returns an object.)

### parseString(individualSetCookieHeader, [options])

Parses a single set-cookie header value string. Options default is `{decodeValues: true}`. Used under-the-hood by `parse()`. 
Returns an object.

### splitCookiesString(combinedSetCookieHeader)

It's uncommon, but the HTTP spec does allow for multiple of the same header to have their values combined (comma-separated) into a single header. 
This method splits apart a combined header without choking on commas that appear within a cookie's value (or expiration date).
Returns an array of strings that may be passed to `parse()`.

## V2 Changes

* Added decodeValues option (calls `decodeURIComponent()` on each cookie value), enabled by default.
* Added `splitCookiesString` method.

## References

* [RFC 6265: HTTP State Management Mechanism](https://tools.ietf.org/html/rfc6265)

## License

MIT © [Nathan Friedly](http://www.nfriedly.com/)


[npm-image]: https://badge.fury.io/js/set-cookie-parser.svg
[npm-url]: https://npmjs.org/package/set-cookie-parser
[travis-image]: https://travis-ci.org/nfriedly/set-cookie-parser.svg?branch=master
[travis-url]: https://travis-ci.org/nfriedly/set-cookie-parser
[daviddm-image]: https://david-dm.org/nfriedly/set-cookie-parser.svg?theme=shields.io
[daviddm-url]: https://david-dm.org/nfriedly/set-cookie-parser
