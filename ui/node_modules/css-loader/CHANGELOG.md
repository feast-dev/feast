# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.

### [6.5.1](https://github.com/webpack-contrib/css-loader/compare/v6.5.0...v6.5.1) (2021-11-03)


### Bug Fixes

* regression with unicode characters in locals ([b7a8441](https://github.com/webpack-contrib/css-loader/commit/b7a84414fb3f6e6ff413cbbb7004fa74a78da331))
* runtime path generation ([#1393](https://github.com/webpack-contrib/css-loader/issues/1393)) ([feafea8](https://github.com/webpack-contrib/css-loader/commit/feafea812a95db05e9b52beaced0def611bba5c8))

## [6.5.0](https://github.com/webpack-contrib/css-loader/compare/v6.4.0...v6.5.0) (2021-10-26)


### Features

* support absolute URL in `url()` when `experiments.buildHttp` enabled ([#1389](https://github.com/webpack-contrib/css-loader/issues/1389)) ([8946be4](https://github.com/webpack-contrib/css-loader/commit/8946be4d0f2c0237cd5fa846d67d469ff20058a4))


### Bug Fixes

* respect `nosources` in the `devtool` option ([c60eff2](https://github.com/webpack-contrib/css-loader/commit/c60eff212337c8a65995f6675d25f49bb515e77d))

## [6.4.0](https://github.com/webpack-contrib/css-loader/compare/v6.3.0...v6.4.0) (2021-10-09)


### Features

* generate more collision resistant for locals ([c7db752](https://github.com/webpack-contrib/css-loader/commit/c7db752fe6a9c7ff28d165fd24a37be08ef83af5))


### Bug Fixes

* classes generation for client and server bundling ([303a3a1](https://github.com/webpack-contrib/css-loader/commit/303a3a171793cf1044c131e291f5c29f9ab86c77))

## [6.3.0](https://github.com/webpack-contrib/css-loader/compare/v6.2.0...v6.3.0) (2021-09-18)


### Features

* added `[folder]` placeholder ([a0dee4f](https://github.com/webpack-contrib/css-loader/commit/a0dee4fd34dd1b9892dac7645a4e57ec134e561b))
* added the `exportType` option with `'array'`, `'string'` and `'css-style-sheet'` values ([c6d2066](https://github.com/webpack-contrib/css-loader/commit/c6d20664ca03226ace26b9766e484e437ec74f60))
  * `'array'` - the default export is `Array` with API for `style-loader` and other
  * `'string'` - the default export is `String` you don't need [`to-string-loader`](https://www.npmjs.com/package/to-string-loader) loader anymore
  * `'css-style-sheet'` - the default export is a [`constructable stylesheet`](https://developers.google.com/web/updates/2019/02/constructable-stylesheets), you can use `import sheet from './styles.css' assert { type: 'css' };` like in a browser, more information you can find [here](https://github.com/webpack-contrib/css-loader#css-style-sheet)
* supported `supports()` and `layer()` functions in `@import` at-rules ([#1377](https://github.com/webpack-contrib/css-loader/issues/1377)) ([bce2c17](https://github.com/webpack-contrib/css-loader/commit/bce2c17524290591be243829187f909a0ae5a6f7))
* fix multiple merging multiple `@media` at-rules ([#1377](https://github.com/webpack-contrib/css-loader/issues/1377)) ([bce2c17](https://github.com/webpack-contrib/css-loader/commit/bce2c17524290591be243829187f909a0ae5a6f7))


### Bug Fixes

* reduce runtime ([#1378](https://github.com/webpack-contrib/css-loader/issues/1378)) ([cf3a3a7](https://github.com/webpack-contrib/css-loader/commit/cf3a3a7346aa73637ee6aae6fef5648965c31a47))

## [6.2.0](https://github.com/webpack-contrib/css-loader/compare/v6.1.0...v6.2.0) (2021-07-19)


### Features

* allow the `exportLocalsConvention` option can be a function, useful for named export ([#1351](https://github.com/webpack-contrib/css-loader/issues/1351)) ([3c4b357](https://github.com/webpack-contrib/css-loader/commit/3c4b35718273baaf9e0480db715b596fbe5d7453))

## [6.1.0](https://github.com/webpack-contrib/css-loader/compare/v6.0.0...v6.1.0) (2021-07-17)


### Features

* add `link` in schema ([#1345](https://github.com/webpack-contrib/css-loader/issues/1345)) ([7d4e493](https://github.com/webpack-contrib/css-loader/commit/7d4e4931390f9e9356af45ae03057d1505d73109))


### Bug Fixes

* respect the `localIdentRegExp` option ([#1349](https://github.com/webpack-contrib/css-loader/issues/1349)) ([42f150b](https://github.com/webpack-contrib/css-loader/commit/42f150b429afad9b0851d2e6bd75cec120885aa4))

## [6.0.0](https://github.com/webpack-contrib/css-loader/compare/v5.2.7...v6.0.0) (2021-07-14)

### Notes

* using `~` is deprecated when the `esModules` option is enabled (enabled by default) and can be removed from your code (**we recommend it**) (`url(~package/image.png)` -> `url(package/image.png)`, `@import url(~package/style.css)` -> `@import url(package/style.css)`, `composes: import from '~package/one.css';` -> `composes: import from 'package/one.css';`), but we still support it for historical reasons. Why can you remove it? The loader will first try to resolve `@import`/`url()`/etc as relative, if it cannot be resolved, the loader will try to resolve `@import`/`url()`/etc inside [`node_modules` or modules directories](https://webpack.js.org/configuration/resolve/#resolvemodules).
* `file-loader` and `url-loader` are deprecated, please migrate on [`asset modules`](https://webpack.js.org/guides/asset-modules/), since v6 `css-loader` is generating `new URL(...)` syntax, it enables by default built-in [`assets modules`](https://webpack.js.org/guides/asset-modules/), i.e. `type: 'asset'` for all `url()`

### ⚠ BREAKING CHANGES

* minimum supported `Node.js` version is `12.13.0`
* minimum supported `webpack` version is `5`, we recommend to update to the latest version for better performance
* for `url` and `import` options `Function` type was removed in favor `Object` type with the `filter` property, i.e. before `{ url: () => true }`, now `{ url: { filter: () => true } }` and  before `{ import: () => true }`, now `{ import: { filter: () => true } }`
* the `modules.compileType` option was removed in favor the `modules.mode` option with `icss` value, also the `modules` option can have `icss` string value
* `new URL()` syntax used for `url()`, only when the `esModules` option is enabled (enabled by default), it means you can bundle CSS for libraries
* [data URI](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/Data_URIs) are handling in `url()`, it means you can register loaders for them, [example](https://webpack.js.org/configuration/module/#rulescheme)
* aliases with `false` value for `url()` now generate empty data URI (i.e. `data:0,`), only when the `esModules` option is enabled (enabled by default)
* `[ext]` placeholder don't need `.` (dot) before for the `localIdentName` option, i.e. please change `.[ext]` on `[ext]` (no dot before) 
* `[folder]` placeholder was removed without replacement for the `localIdentName` option, please use a custom function if you need complex logic
* `[emoji]` placeholder was removed without replacement for the `localIdentName` option, please use a custom function if you need complex logic
* the `localIdentHashPrefix` was removed in favor the `localIdentHashSalt` option

### Features

* supported [`resolve.byDependency.css`](https://webpack.js.org/configuration/resolve/#resolvebydependency) resolve options for `@import`
* supported [`resolve.byDependency.icss`](https://webpack.js.org/configuration/resolve/#resolvebydependency) resolve CSS modules and ICSS imports (i.e. `composes`/etc)
* added `modules.localIdentHashFunction`, `modules.localIdentHashDigest`, `modules.localIdentHashDigestLength` options for better class hashing controlling
* less dependencies

### Bug Fixes

* better performance
* fixed circular `@import`

### Notes

* **we strongly recommend not to add `.css` to `resolve.extensions`, it reduces performance and in most cases it is simply not necessary, alternative you can set resolve options [by dependency](https://webpack.js.org/configuration/resolve/#resolvebydependency)**   

### [5.2.7](https://github.com/webpack-contrib/css-loader/compare/v5.2.6...v5.2.7) (2021-07-13)


### Bug Fixes

* fix crash when source map is unavailable with external URL in `[@import](https://github.com/import)` ([bb76fe4](https://github.com/webpack-contrib/css-loader/commit/bb76fe48a198e74cacf29ad4b1c01d485f4db11f))

### [5.2.6](https://github.com/webpack-contrib/css-loader/compare/v5.2.5...v5.2.6) (2021-05-24)


### Bug Fixes

* always write locals export when css modules/icss enabled ([#1315](https://github.com/webpack-contrib/css-loader/issues/1315)) ([075d9bd](https://github.com/webpack-contrib/css-loader/commit/075d9bd044a78543479cbf10ccd3c386a3e434e6))

### [5.2.5](https://github.com/webpack-contrib/css-loader/compare/v5.2.4...v5.2.5) (2021-05-20)


### Bug Fixes

* compatibility with named export and es5 ([#1314](https://github.com/webpack-contrib/css-loader/issues/1314)) ([0cf8cde](https://github.com/webpack-contrib/css-loader/commit/0cf8cdedd8667b1ba13d3b4322087943a25176f6))

### [5.2.4](https://github.com/webpack-contrib/css-loader/compare/v5.2.3...v5.2.4) (2021-04-19)


### Bug Fixes

* do not crash on 'false' aliases ([#1292](https://github.com/webpack-contrib/css-loader/issues/1292)) ([e913cb1](https://github.com/webpack-contrib/css-loader/commit/e913cb1d73a4f5c3c4464e0446a885e9f677a005))

### [5.2.3](https://github.com/webpack-contrib/css-loader/compare/v5.2.2...v5.2.3) (2021-04-19)

### Bug Fixes

* improve performance

### [5.2.2](https://github.com/webpack-contrib/css-loader/compare/v5.2.1...v5.2.2) (2021-04-16)


### Bug Fixes

* avoid escape nonASCII characters in local names ([0722733](https://github.com/webpack-contrib/css-loader/commit/072273308a8ab4b7efdae31440689dc81978ca1d))

### [5.2.1](https://github.com/webpack-contrib/css-loader/compare/v5.2.0...v5.2.1) (2021-04-09)


### Bug Fixes

* do not crash on unescaped svg data uri ([#1288](https://github.com/webpack-contrib/css-loader/issues/1288)) ([4f289c5](https://github.com/webpack-contrib/css-loader/commit/4f289c5e4df6c666fdf6dd3402560ae74d4bf7ee))

## [5.2.0](https://github.com/webpack-contrib/css-loader/compare/v5.1.4...v5.2.0) (2021-03-24)


### Features

* support async functions for `url` and `import` options ([#1277](https://github.com/webpack-contrib/css-loader/issues/1277)) ([c5062db](https://github.com/webpack-contrib/css-loader/commit/c5062db3fc849d882a07b9f2c9f66f00325c8896))

### [5.1.4](https://github.com/webpack-contrib/css-loader/compare/v5.1.3...v5.1.4) (2021-03-24)


### Bug Fixes

* crash with thread-loader ([#1281](https://github.com/webpack-contrib/css-loader/issues/1281)) ([7095a7c](https://github.com/webpack-contrib/css-loader/commit/7095a7ca7d985d5447aed80cf3e41a4f8c19b954))

### [5.1.3](https://github.com/webpack-contrib/css-loader/compare/v5.1.2...v5.1.3) (2021-03-15)


### Bug Fixes

* the `auto` option works using inline module syntax ([#1274](https://github.com/webpack-contrib/css-loader/issues/1274)) ([1db2f4d](https://github.com/webpack-contrib/css-loader/commit/1db2f4df3ff9ae8f0667a2304853c8e7cdd0afc1))
* ident generation for CSS modules using inline module syntax ([#1274](https://github.com/webpack-contrib/css-loader/issues/1274)) ([1db2f4d](https://github.com/webpack-contrib/css-loader/commit/1db2f4df3ff9ae8f0667a2304853c8e7cdd0afc1))

### [5.1.2](https://github.com/webpack-contrib/css-loader/compare/v5.1.1...v5.1.2) (2021-03-10)


### Bug Fixes

* handling `@import` with spaces before and after and any extensions ([#1272](https://github.com/webpack-contrib/css-loader/issues/1272)) ([0c47cf7](https://github.com/webpack-contrib/css-loader/commit/0c47cf7ccbe3635900e8e8840650f69a7eca004d))
* inline loader syntax in `@import` and modules ([3f49ed0](https://github.com/webpack-contrib/css-loader/commit/3f49ed0864457f9467f560856377c890c392aee7))

### [5.1.1](https://github.com/webpack-contrib/css-loader/compare/v5.1.0...v5.1.1) (2021-03-01)


### Bug Fixes

* crash on modified AST from `postcss-loader` ([#1268](https://github.com/webpack-contrib/css-loader/issues/1268)) ([d2a1a84](https://github.com/webpack-contrib/css-loader/commit/d2a1a84afc63fdfb2a4ce6668ed9f2d7f1ba56ca))

## [5.1.0](https://github.com/webpack-contrib/css-loader/compare/v5.0.2...v5.1.0) (2021-02-25)


### Features

* added support webpackIgnore comment ([#1264](https://github.com/webpack-contrib/css-loader/issues/1264)) ([53d40a9](https://github.com/webpack-contrib/css-loader/commit/53d40a9bb35a79e6a15308bbb7a01358f39816df))

### [5.0.2](https://github.com/webpack-contrib/css-loader/compare/v5.0.1...v5.0.2) (2021-02-08)


### Bug Fixes

* pass query with hash to other loaders ([#1261](https://github.com/webpack-contrib/css-loader/issues/1261)) ([729a314](https://github.com/webpack-contrib/css-loader/commit/729a314529cd0607c374b07bdf425337f9a778d4))

### [5.0.1](https://github.com/webpack-contrib/css-loader/compare/v5.0.0...v5.0.1) (2020-11-04)


### Bug Fixes

* sources in source maps have relative paths ([#1219](https://github.com/webpack-contrib/css-loader/issues/1219)) ([3229b3c](https://github.com/webpack-contrib/css-loader/commit/3229b3cca3cb5d762daeff57239a965b06fd7593))

## [5.0.0](https://github.com/webpack-contrib/css-loader/compare/v4.3.0...v5.0.0) (2020-10-13)


### ⚠ BREAKING CHANGES

* migrate on PostCSS 8
* runtime doesn't contain source maps code without `sourceMap: true`
* returned value from the `getLocalIdent` escapes by default, the `exportName` value is always unescaped
* Auto enable icss modules for all files for which `/\.icss\.\w+$/i` (the `modules.compileType` option is `icss`)
* `[emoji]` placeholder was deprecated
* `icss` option was removed (it was deprecated previously)

### Features

* allow named exports to have underscores in names ([#1209](https://github.com/webpack-contrib/css-loader/issues/1209)) ([747d62b](https://github.com/webpack-contrib/css-loader/commit/747d62b75a878d8881f4819b96297667dc689b8f))
* hide warning when you don't need handle `url()`/`@import` ([#1195](https://github.com/webpack-contrib/css-loader/issues/1195)) ([dd52931](https://github.com/webpack-contrib/css-loader/commit/dd52931150ed42f122d9017642437c26cc1b2422))
* improve error message  ([52412f6](https://github.com/webpack-contrib/css-loader/commit/52412f6d5a54745ee37a4a67f038455c26ba5772))
* reduce runtime ([9f974be](https://github.com/webpack-contrib/css-loader/commit/9f974be81f5942d3afaf783529677bd541952fa3))
* add fallback if custom getLocalIdent returns `null`/`undefined` ([#1193](https://github.com/webpack-contrib/css-loader/issues/1193)) ([0f95841](https://github.com/webpack-contrib/css-loader/commit/0f9584135e63f9f354043e7f414e0c1aad0edc6e))

## [4.3.0](https://github.com/webpack-contrib/css-loader/compare/v4.2.2...v4.3.0) (2020-09-08)


### Features

* the `importLoaders` can be `string` ([#1178](https://github.com/webpack-contrib/css-loader/issues/1178)) ([ec58a7c](https://github.com/webpack-contrib/css-loader/commit/ec58a7cfda46443e35539d66b86685195fa5db03))


### Bug Fixes

* line breaks in `url` function ([88b8ddc](https://github.com/webpack-contrib/css-loader/commit/88b8ddc1d78a2b6a917ed2dfe2f2a37cf6a84190))

### [4.2.2](https://github.com/webpack-contrib/css-loader/compare/v4.2.1...v4.2.2) (2020-08-24)


### Bug Fixes

* source maps generation, source from source maps are now relative to `compiler.context` and use `webpack://` protocol ([#1169](https://github.com/webpack-contrib/css-loader/issues/1169)) ([fb5c53d](https://github.com/webpack-contrib/css-loader/commit/fb5c53d80b10ee698762238bb7b122aec8c5048d))

### [4.2.1](https://github.com/webpack-contrib/css-loader/compare/v4.2.0...v4.2.1) (2020-08-06)


### Bug Fixes

* regression with the `exportOnlyLocals` option, now `locals` are not exported under the `locals` name, it was big regression, we apologize for that ([24c0a12](https://github.com/webpack-contrib/css-loader/commit/24c0a122d1396c08326a24f6184f5da09cf52ccc))

## [4.2.0](https://github.com/webpack-contrib/css-loader/compare/v4.1.1...v4.2.0) (2020-07-31)


### Features

* add `module.type` option, the `icss` option is deprecated ([#1150](https://github.com/webpack-contrib/css-loader/issues/1150)) ([68f72af](https://github.com/webpack-contrib/css-loader/commit/68f72af2a09111f74dcacbf7af019fe7eb40cb6c))

### [4.1.1](https://github.com/webpack-contrib/css-loader/compare/v4.1.0...v4.1.1) (2020-07-30)


### Bug Fixes

* remove unnecessary `console` call ([#1148](https://github.com/webpack-contrib/css-loader/issues/1148)) ([b1b90ca](https://github.com/webpack-contrib/css-loader/commit/b1b90caaea8eb045177749729340c7906454a84b))

## [4.1.0](https://github.com/webpack-contrib/css-loader/compare/v4.0.0...v4.1.0) (2020-07-29)


### Features

* add `icss` option ([#1140](https://github.com/webpack-contrib/css-loader/issues/1140)) ([a8ec7da](https://github.com/webpack-contrib/css-loader/commit/a8ec7da42234e0b2eb061d2a920669940bcbdf05))
* support absolute paths ([f9ba0ce](https://github.com/webpack-contrib/css-loader/commit/f9ba0ce11789770c4c9220478e9c98dbd432a5d6))


### Bug Fixes

* do not crash with `data` URLs ([#1142](https://github.com/webpack-contrib/css-loader/issues/1142)) ([91bc64b](https://github.com/webpack-contrib/css-loader/commit/91bc64b81abfeffd174639a8fdf2366412c11426))
* performance ([#1144](https://github.com/webpack-contrib/css-loader/issues/1144)) ([4f1baa2](https://github.com/webpack-contrib/css-loader/commit/4f1baa211eb27b0b281ba9f262fa12e8aaefc0ba))

## [4.0.0](https://github.com/webpack-contrib/css-loader/compare/v3.6.0...v4.0.0) (2020-07-25)


### ⚠ BREAKING CHANGES

* minimum required `Node.js` version is `10.13.0`
* minimum required `webpack` version is `4.27.0`
* the `esModule` option is `true` by default
* default value of the `sourceMap` option depends on the `devtool` option
* `icss` plugin disable by default, you need to setup the `modules` option to enable it
* the `modules` option is `true` by default for all files matching `/\.module\.\w+$/i.test(filename)` regular expression, `module.auto` is `true` by default
* the `modules.context` option was renamed to the `modules.localIdentContext` option
* default the `modules.localIdentContext` value is `compiler.context` for the `module.getLocalIdent` option
* the `modules.hashPrefix` option was renamed to the `modules.localIdentHashPrefix` option
* the `localsConvention` option was moved and renamed to the `modules.exportLocalsConvention` option
* the `getLocalIndent` option should be always `Function` and should always return `String` value
* the `onlyLocals` option was moved and renamed to the `modules.exportOnlyLocals` option
* function arguments of the `import` option were changed, it is now `function(url, media, resourcePath) {}`
* inline syntax was changed, please write `~` before the file request, i.e. rewrite `url(~!!loader!package/img.png)` to `url(!!loader!~package/img.png)`
 * `url()` resolving algorithm now handles absolute paths instead of ignoring them. This can break builds which relied on absolute paths to refer to the asset directory. ([bc19ddd](https://github.com/webpack-contrib/css-loader/commit/bc19ddd8779dafbc2a420870a3cb841041ce9c7c))

### Features

* `@value` supports importing `url()` ([#1126](https://github.com/webpack-contrib/css-loader/issues/1126)) ([7f49a0a](https://github.com/webpack-contrib/css-loader/commit/7f49a0a6047846bb2e432558365e19d4a0dfb366))
* improve `url()` resolving algorithm to support more path types ([bc19ddd](https://github.com/webpack-contrib/css-loader/commit/bc19ddd8779dafbc2a420870a3cb841041ce9c7c))
* named export for locals ([#1108](https://github.com/webpack-contrib/css-loader/issues/1108)) ([d139ec1](https://github.com/webpack-contrib/css-loader/commit/d139ec1d763f9944550b31f2a75183e488dd1224))
* respected the `style` field from package.json ([#1099](https://github.com/webpack-contrib/css-loader/issues/1099)) ([edf5347](https://github.com/webpack-contrib/css-loader/commit/edf5347e4203a62e50b87248a83da198afdc6eba))
* support `file:` protocol ([5604205](https://github.com/webpack-contrib/css-loader/commit/560420567eb0e1a635648b7f4ff0365db475384c))
* support server relative URLs

### Bug Fixes

* resolution algorithm, you don't need `~` inside packages in `node_modules` ([76f1480](https://github.com/webpack-contrib/css-loader/commit/76f1480b14265369ac5dc8dbbce467cfb8e814c5))


## [3.6.0](https://github.com/webpack-contrib/css-loader/compare/v3.5.3...v3.6.0) (2020-06-13)


### Features

* allow `modules.auto` to be a filter function ([#1086](https://github.com/webpack-contrib/css-loader/issues/1086)) ([0902353](https://github.com/webpack-contrib/css-loader/commit/0902353c328d4d18e8ed2755fe9c83c03c53df81))

### [3.5.3](https://github.com/webpack-contrib/css-loader/compare/v3.5.2...v3.5.3) (2020-04-24)


### Bug Fixes

* add file from an error to file dependencies ([841423f](https://github.com/webpack-contrib/css-loader/commit/841423fca2932c18f8ac0cf0a1f0012fc0a62fb6))
* avoid query string in source maps ([#1082](https://github.com/webpack-contrib/css-loader/issues/1082)) ([f64de13](https://github.com/webpack-contrib/css-loader/commit/f64de13f7377eff9501348cf26213212ca696913))

### [3.5.2](https://github.com/webpack-contrib/css-loader/compare/v3.5.1...v3.5.2) (2020-04-10)


### Bug Fixes

* schema for the `modules.auto` option ([#1075](https://github.com/webpack-contrib/css-loader/issues/1075)) ([8c9ffe7](https://github.com/webpack-contrib/css-loader/commit/8c9ffe7c6df11232b63173c757baa71ed36f6145))

### [3.5.1](https://github.com/webpack-contrib/css-loader/compare/v3.5.0...v3.5.1) (2020-04-07)


### Bug Fixes

* don't generate an invalid code for `locals` ([#1072](https://github.com/webpack-contrib/css-loader/issues/1072)) ([866b84a](https://github.com/webpack-contrib/css-loader/commit/866b84acd7fd47651f741ca1e6cf7081c2bbe357))

## [3.5.0](https://github.com/webpack-contrib/css-loader/compare/v3.4.2...v3.5.0) (2020-04-06)


### Features

* accept semver compatible postcss AST ([#1049](https://github.com/webpack-contrib/css-loader/issues/1049)) ([14c4faa](https://github.com/webpack-contrib/css-loader/commit/14c4faae87305c9b965de4f468bb1e118f6b84cc))
* allow to determinate css modules using the `modules.auto` option, please look at an [example](https://github.com/webpack-contrib/css-loader#pure-css-css-modules-and-postcss) of how you can simplify the configuration. ([#1067](https://github.com/webpack-contrib/css-loader/issues/1067)) ([c673cf4](https://github.com/webpack-contrib/css-loader/commit/c673cf418e901c5050bc697eb45401dc9a42c477))
* the `modules.exportGlobals` option for export global classes and ids ([#1069](https://github.com/webpack-contrib/css-loader/issues/1069)) ([519e5f4](https://github.com/webpack-contrib/css-loader/commit/519e5f41539f4c87ec96db0a908aaadecc284a6c))
* the `modules.mode` option may be a function ([#1065](https://github.com/webpack-contrib/css-loader/issues/1065)) ([0d8ac3b](https://github.com/webpack-contrib/css-loader/commit/0d8ac3bcb831bc747657c914aba106b93840737e))

### [3.4.2](https://github.com/webpack-contrib/css-loader/compare/v3.4.1...v3.4.2) (2020-01-10)


### Bug Fixes

* do not duplicate css on `composes` ([#1040](https://github.com/webpack-contrib/css-loader/issues/1040)) ([df79602](https://github.com/webpack-contrib/css-loader/commit/df7960277be20ec80e9be1a41ac53baf69847fa0))

### [3.4.1](https://github.com/webpack-contrib/css-loader/compare/v3.4.0...v3.4.1) (2020-01-03)


### Bug Fixes

* do not output `undefined` when sourceRoot is unavailable ([#1036](https://github.com/webpack-contrib/css-loader/issues/1036)) ([ded2a79](https://github.com/webpack-contrib/css-loader/commit/ded2a797271f2adf864bf92300621c024974bc79))
* don't output invalid es5 code when locals do not exists ([#1035](https://github.com/webpack-contrib/css-loader/issues/1035)) ([b60e62a](https://github.com/webpack-contrib/css-loader/commit/b60e62a655719cc1779fae7d577af6ad6cf42135))

## [3.4.0](https://github.com/webpack-contrib/css-loader/compare/v3.3.1...v3.4.0) (2019-12-17)


### Features

* `esModule` option ([#1026](https://github.com/webpack-contrib/css-loader/issues/1026)) ([d358cdb](https://github.com/webpack-contrib/css-loader/commit/d358cdbe2c026afafa0279003cb6c8a3eff4c419))


### Bug Fixes

* logic for order and media queries for imports ([#1018](https://github.com/webpack-contrib/css-loader/issues/1018)) ([65450d9](https://github.com/webpack-contrib/css-loader/commit/65450d9c04790ccc9fb06eac81ea6d8f3cdbfaac))

### [3.3.2](https://github.com/webpack-contrib/css-loader/compare/v3.3.1...v3.3.2) (2019-12-12)


### Bug Fixes

* logic for order and media queries for imports ([1fb5134](https://github.com/webpack-contrib/css-loader/commit/1fb51340a7719b6f5b517cb71ea85ec5d45c1199))

### [3.3.1](https://github.com/webpack-contrib/css-loader/compare/v3.3.0...v3.3.1) (2019-12-12)


### Bug Fixes

* better handling url functions and an url in `@import` at-rules
* reduce count of `require` ([#1014](https://github.com/webpack-contrib/css-loader/issues/1014)) ([e091d27](https://github.com/webpack-contrib/css-loader/commit/e091d2709c29ac57ed0106af8ec3b581cbda7a9c))

## [3.3.0](https://github.com/webpack-contrib/css-loader/compare/v3.2.1...v3.3.0) (2019-12-09)


### Features

* support `pure` css modules ([#1008](https://github.com/webpack-contrib/css-loader/issues/1008)) ([6177af5](https://github.com/webpack-contrib/css-loader/commit/6177af5596566fead13a8f66d5abcb4dc2b744db))


### Bug Fixes

* do not crash when an assert return `null` or `undefined` ([#1006](https://github.com/webpack-contrib/css-loader/issues/1006)) ([6769783](https://github.com/webpack-contrib/css-loader/commit/67697833725e1cff12a14663390bbe4c65ea36d2))
* reduce count of `require` ([#1004](https://github.com/webpack-contrib/css-loader/issues/1004)) ([80e9662](https://github.com/webpack-contrib/css-loader/commit/80e966280f2477c5c0e4553d3be3a04511fea381))

### [3.2.1](https://github.com/webpack-contrib/css-loader/compare/v3.2.0...v3.2.1) (2019-12-02)


### Bug Fixes

* add an additional space after the escape sequence ([#998](https://github.com/webpack-contrib/css-loader/issues/998)) ([0961304](https://github.com/webpack-contrib/css-loader/commit/0961304020832fc9ca70cc708f4366e1f868e765))
* compatibility with ES modules syntax and hash in `url` function ([#1001](https://github.com/webpack-contrib/css-loader/issues/1001)) ([8f4d6f5](https://github.com/webpack-contrib/css-loader/commit/8f4d6f508187513347106a436eda993f874065f1))

## [3.2.0](https://github.com/webpack-contrib/css-loader/compare/v3.1.0...v3.2.0) (2019-08-06)


### Bug Fixes

* replace `.` characters in localIndent to `-` character (regression) ([#982](https://github.com/webpack-contrib/css-loader/issues/982)) ([967fb66](https://github.com/webpack-contrib/css-loader/commit/967fb66))


### Features

* support es modules for assets loader ([#984](https://github.com/webpack-contrib/css-loader/issues/984)) ([9c5126c](https://github.com/webpack-contrib/css-loader/commit/9c5126c))

## [3.1.0](https://github.com/webpack-contrib/css-loader/compare/v3.0.0...v3.1.0) (2019-07-18)


### Bug Fixes

* converting all (including reserved and control) filesystem characters to `-` (it was regression in `3.0.0` version) ([#972](https://github.com/webpack-contrib/css-loader/issues/972)) ([f51859b](https://github.com/webpack-contrib/css-loader/commit/f51859b))
* default context should be undefined instead of null ([#965](https://github.com/webpack-contrib/css-loader/issues/965)) ([9c32885](https://github.com/webpack-contrib/css-loader/commit/9c32885))


### Features

* allow `modules.getLocalIdent` to return a falsy value ([#963](https://github.com/webpack-contrib/css-loader/issues/963)) ([9c3571c](https://github.com/webpack-contrib/css-loader/commit/9c3571c))
* improved validation error messages ([65e4fc0](https://github.com/webpack-contrib/css-loader/commit/65e4fc0))



## [3.0.0](https://github.com/webpack-contrib/css-loader/compare/v2.1.1...v3.0.0) (2019-06-11)


### Bug Fixes

* avoid the "from" argument must be of type string error ([#908](https://github.com/webpack-contrib/css-loader/issues/908)) ([e5dfd23](https://github.com/webpack-contrib/css-loader/commit/e5dfd23))
* invert `Function` behavior for `url` and `import` options ([#939](https://github.com/webpack-contrib/css-loader/issues/939)) ([e9eb5ad](https://github.com/webpack-contrib/css-loader/commit/e9eb5ad))
* properly export locals with escaped characters ([#917](https://github.com/webpack-contrib/css-loader/issues/917)) ([a0efcda](https://github.com/webpack-contrib/css-loader/commit/a0efcda))
* property handle non css characters in localIdentName ([#920](https://github.com/webpack-contrib/css-loader/issues/920)) ([d3a0a3c](https://github.com/webpack-contrib/css-loader/commit/d3a0a3c))


### Features

* modules options now accepts object config ([#937](https://github.com/webpack-contrib/css-loader/issues/937)) ([1d7a464](https://github.com/webpack-contrib/css-loader/commit/1d7a464))
* support `@value` at-rule in selectors ([#941](https://github.com/webpack-contrib/css-loader/issues/941)) ([05a42e2](https://github.com/webpack-contrib/css-loader/commit/05a42e2))


### BREAKING CHANGES

* minimum required nodejs version is 8.9.0
* `@value` at rules now support in `selector`, recommends checking all `@values` at-rule usage (hint: you can add prefix to all `@value` at-rules, for example `@value v-foo: black;` or `@value m-foo: screen and (max-width: 12450px)`, and then do upgrade)
* invert `{Function}` behavior for `url` and `import` options  (need return `true` when you want handle `url`/`@import` and return `false` if not)
* `camelCase` option was remove in favor `localsConvention` option, also it is accept only `{String}` value (use `camelCase` value if you previously value was `true` and `asIs` if you previously value was `false`)
* `exportOnlyLocals` option was remove in favor `onlyLocals` option
* `modules` option now can be `{Object}` and allow to setup `CSS Modules` options:
  * `localIdentName` option was removed in favor `modules.localIdentName` option
  * `context` option was remove in favor `modules.context` option
  * `hashPrefix` option was removed in favor `modules.hashPrefix` option
  * `getLocalIdent` option was removed in favor `modules.getLocalIdent` option
  * `localIdentRegExp` option was removed in favor `modules.localIdentRegExp` option



<a name="2.1.1"></a>
## [2.1.1](https://github.com/webpack-contrib/css-loader/compare/v2.1.0...v2.1.1) (2019-03-07)


### Bug Fixes

* do not break selector with escaping ([#896](https://github.com/webpack-contrib/css-loader/issues/896)) ([0ba8c66](https://github.com/webpack-contrib/css-loader/commit/0ba8c66))
* source map generation when `sourceRoot` is present ([#901](https://github.com/webpack-contrib/css-loader/issues/901)) ([e9ce745](https://github.com/webpack-contrib/css-loader/commit/e9ce745))
* sourcemap generating when previous loader pass sourcemap as string ([#905](https://github.com/webpack-contrib/css-loader/issues/905)) ([3797e4d](https://github.com/webpack-contrib/css-loader/commit/3797e4d))



<a name="2.1.0"></a>
# [2.1.0](https://github.com/webpack-contrib/css-loader/compare/v2.0.2...v2.1.0) (2018-12-25)


### Features

* support `image-set` without `url` ([#879](https://github.com/webpack-contrib/css-loader/issues/879)) ([21884e2](https://github.com/webpack-contrib/css-loader/commit/21884e2))



<a name="2.0.2"></a>
## [2.0.2](https://github.com/webpack-contrib/css-loader/compare/v2.0.1...v2.0.2) (2018-12-21)


### Bug Fixes

* inappropriate modification of animation keywords ([#876](https://github.com/webpack-contrib/css-loader/issues/876)) ([dfb2f8e](https://github.com/webpack-contrib/css-loader/commit/dfb2f8e))



<a name="2.0.1"></a>
# [2.0.1](https://github.com/webpack-contrib/css-loader/compare/v2.0.0...v2.0.1) (2018-12-14)


### Bug Fixes

* safe checking if params are present for at rule ([#871](https://github.com/webpack-contrib/css-loader/issues/871)) ([a88fed1](https://github.com/webpack-contrib/css-loader/commit/a88fed1))
* `getLocalIdent` now accepts `false` value ([#865](https://github.com/webpack-contrib/css-loader/issues/865)) ([1825e8a](https://github.com/webpack-contrib/css-loader/commit/1825e8a))



<a name="2.0.0"></a>
# [2.0.0](https://github.com/webpack-contrib/css-loader/compare/v1.0.1...v2.0.0) (2018-12-07)


### Bug Fixes

* broken unucode characters ([#850](https://github.com/webpack-contrib/css-loader/issues/850)) ([f599c70](https://github.com/webpack-contrib/css-loader/commit/f599c70))
* correctly processing `urls()` with `?#hash` ([#803](https://github.com/webpack-contrib/css-loader/issues/803)) ([417d105](https://github.com/webpack-contrib/css-loader/commit/417d105))
* don't break loader on invalid or not exists url or import token ([#827](https://github.com/webpack-contrib/css-loader/issues/827)) ([9e52d26](https://github.com/webpack-contrib/css-loader/commit/9e52d26))
* don't duplicate import with same media in different case ([#819](https://github.com/webpack-contrib/css-loader/issues/819)) ([9f66e33](https://github.com/webpack-contrib/css-loader/commit/9f66e33))
* emit warnings on broken `import` at-rules ([#806](https://github.com/webpack-contrib/css-loader/issues/806)) ([4bdf08b](https://github.com/webpack-contrib/css-loader/commit/4bdf08b))
* handle uppercase `URL` in `import` at-rules ([#818](https://github.com/webpack-contrib/css-loader/issues/818)) ([3ebdcd5](https://github.com/webpack-contrib/css-loader/commit/3ebdcd5))
* inconsistent generate class names for css modules on difference os ([#812](https://github.com/webpack-contrib/css-loader/issues/812)) ([0bdf9b7](https://github.com/webpack-contrib/css-loader/commit/0bdf9b7))
* reduce number of `require` for `urls()` ([#854](https://github.com/webpack-contrib/css-loader/issues/854)) ([3338656](https://github.com/webpack-contrib/css-loader/commit/3338656))
* support deduplication of string module ids (optimization.namedModules) ([#789](https://github.com/webpack-contrib/css-loader/issues/789)) ([e3bb83a](https://github.com/webpack-contrib/css-loader/commit/e3bb83a))
* support module resolution in `composes` ([#845](https://github.com/webpack-contrib/css-loader/issues/845)) ([453248f](https://github.com/webpack-contrib/css-loader/commit/453248f))
* same `urls()` resolving logic for `modules` (`local` and `global`) and without modules ([#843](https://github.com/webpack-contrib/css-loader/issues/843)) ([fdcf687](https://github.com/webpack-contrib/css-loader/commit/fdcf687))

### Features

* allow to disable css modules and **disable their by default** ([#842](https://github.com/webpack-contrib/css-loader/issues/842)) ([889dc7f](https://github.com/webpack-contrib/css-loader/commit/889dc7f))
* disable `import` option doesn't affect on `composes` ([#822](https://github.com/webpack-contrib/css-loader/issues/822)) ([f9aa73c](https://github.com/webpack-contrib/css-loader/commit/f9aa73c))
* allow to filter `urls` ([#856](https://github.com/webpack-contrib/css-loader/issues/856)) ([5e702e7](https://github.com/webpack-contrib/css-loader/commit/5e702e7))
* allow to filter `import` at-rules ([#857](https://github.com/webpack-contrib/css-loader/issues/857)) ([5e6034c](https://github.com/webpack-contrib/css-loader/commit/5e6034c))
* emit warning on invalid `urls()` ([#832](https://github.com/webpack-contrib/css-loader/issues/832)) ([da95db8](https://github.com/webpack-contrib/css-loader/commit/da95db8))
* added `exportOnlyLocals` option ([#824](https://github.com/webpack-contrib/css-loader/issues/824)) ([e9327c0](https://github.com/webpack-contrib/css-loader/commit/e9327c0))
* reuse `postcss` ast from other loaders (i.e `postcss-loader`) ([#840](https://github.com/webpack-contrib/css-loader/issues/840)) ([1dad1fb](https://github.com/webpack-contrib/css-loader/commit/1dad1fb))
* schema options ([b97d997](https://github.com/webpack-contrib/css-loader/commit/b97d997))


### BREAKING CHANGES

* resolving logic for `url()` and `import` at-rules works the same everywhere, it does not matter whether css modules are enabled (with `global` and `local` module) or not. Examples - `url('image.png')` as `require('./image.png')`, `url('./image.png')` as `require('./image.png')`, `url('~module/image.png')` as `require('module/image.png')`.
* by default css modules are disabled (now `modules: false` disable all css modules features), you can return old behaviour change this on `modules: 'global'`
* `css-loader/locals` was dropped in favor `exportOnlyLocals` option
* `import` option only affect on `import` at-rules and doesn't affect on `composes` declarations
* invalid `@import` at rules now emit warnings
* use `postcss@7`



<a name="1.0.1"></a>
## [1.0.1](https://github.com/webpack-contrib/css-loader/compare/v1.0.0...v1.0.1) (2018-10-29)


### Bug Fixes

* **loader:** trim unquoted import urls ([#783](https://github.com/webpack-contrib/css-loader/issues/783)) ([21fcddf](https://github.com/webpack-contrib/css-loader/commit/21fcddf))



<a name="1.0.0"></a>
# [1.0.0](https://github.com/webpack-contrib/css-loader/compare/v0.28.11...v1.0.0) (2018-07-06)


### BREAKING CHANGES

* remove `minimize` option, use [`postcss-loader`](https://github.com/postcss/postcss-loader) with [`cssnano`](https://github.com/cssnano/cssnano) or use [`optimize-cssnano-plugin`](https://github.com/intervolga/optimize-cssnano-plugin) plugin
* remove `module` option, use `modules` option instead
* remove `camelcase` option, use `camelCase` option instead
* remove `root` option, use [`postcss-loader`](https://github.com/postcss/postcss-loader) with [`postcss-url`](https://github.com/postcss/postcss-url) plugin
* remove `alias` option, use [`resolve.alias`](https://webpack.js.org/configuration/resolve/) feature or use [`postcss-loader`](https://github.com/postcss/postcss-loader) with [`postcss-url`](https://github.com/postcss/postcss-url) plugin
* update `postcss` to `6` version
* minimum require `nodejs` version is `6.9`
* minimum require `webpack` version is `4`



<a name="0.28.11"></a>
## [0.28.11](https://github.com/webpack-contrib/css-loader/compare/v0.28.10...v0.28.11) (2018-03-16)


### Bug Fixes

* **lib/processCss:** don't check `mode` for `url` handling (`options.modules`) ([#698](https://github.com/webpack-contrib/css-loader/issues/698)) ([c788450](https://github.com/webpack-contrib/css-loader/commit/c788450))



<a name="0.28.10"></a>
## [0.28.10](https://github.com/webpack-contrib/css-loader/compare/v0.28.9...v0.28.10) (2018-02-22)


### Bug Fixes

* **getLocalIdent:** add `rootContext` support (`webpack >= v4.0.0`) ([#681](https://github.com/webpack-contrib/css-loader/issues/681)) ([9f876d2](https://github.com/webpack-contrib/css-loader/commit/9f876d2))



<a name="0.28.9"></a>
## [0.28.9](https://github.com/webpack-contrib/css-loader/compare/v0.28.8...v0.28.9) (2018-01-17)


### Bug Fixes

* ignore invalid URLs (`url()`) ([#663](https://github.com/webpack-contrib/css-loader/issues/663)) ([d1d8221](https://github.com/webpack-contrib/css-loader/commit/d1d8221))



<a name="0.28.8"></a>
## [0.28.8](https://github.com/webpack-contrib/css-loader/compare/v0.28.7...v0.28.8) (2018-01-05)


### Bug Fixes

* **loader:** correctly check if source map is `undefined` ([#641](https://github.com/webpack-contrib/css-loader/issues/641)) ([0dccfa9](https://github.com/webpack-contrib/css-loader/commit/0dccfa9))
* proper URL escaping and wrapping (`url()`) ([#627](https://github.com/webpack-contrib/css-loader/issues/627)) ([8897d44](https://github.com/webpack-contrib/css-loader/commit/8897d44))



<a name="0.28.7"></a>
## [0.28.7](https://github.com/webpack/css-loader/compare/v0.28.6...v0.28.7) (2017-08-30)


### Bug Fixes

* pass resolver to `localsLoader` (`options.alias`)  ([#601](https://github.com/webpack/css-loader/issues/601)) ([8f1b57c](https://github.com/webpack/css-loader/commit/8f1b57c))



<a name="0.28.6"></a>
## [0.28.6](https://github.com/webpack/css-loader/compare/v0.28.5...v0.28.6) (2017-08-30)


### Bug Fixes

* add support for aliases starting with `/` (`options.alias`) ([#597](https://github.com/webpack/css-loader/issues/597)) ([63567f2](https://github.com/webpack/css-loader/commit/63567f2))



<a name="0.28.5"></a>
## [0.28.5](https://github.com/webpack/css-loader/compare/v0.28.4...v0.28.5) (2017-08-17)


### Bug Fixes

* match mutliple dashes (`options.camelCase`) ([#556](https://github.com/webpack/css-loader/issues/556)) ([1fee601](https://github.com/webpack/css-loader/commit/1fee601))
* stricter `[@import](https://github.com/import)` tolerance ([#593](https://github.com/webpack/css-loader/issues/593)) ([2e4ec09](https://github.com/webpack/css-loader/commit/2e4ec09))



<a name="0.28.4"></a>
## [0.28.4](https://github.com/webpack/css-loader/compare/v0.28.3...v0.28.4) (2017-05-30)


### Bug Fixes

* preserve leading underscore in class names ([#543](https://github.com/webpack/css-loader/issues/543)) ([f6673c8](https://github.com/webpack/css-loader/commit/f6673c8))



<a name="0.28.3"></a>
## [0.28.3](https://github.com/webpack/css-loader/compare/v0.28.2...v0.28.3) (2017-05-25)


### Bug Fixes

* correct plugin order for CSS Modules ([#534](https://github.com/webpack/css-loader/issues/534)) ([b90f492](https://github.com/webpack/css-loader/commit/b90f492))



<a name="0.28.2"></a>
## [0.28.2](https://github.com/webpack/css-loader/compare/v0.28.1...v0.28.2) (2017-05-22)


### Bug Fixes

* source maps path on `windows` ([#532](https://github.com/webpack/css-loader/issues/532)) ([c3d0d91](https://github.com/webpack/css-loader/commit/c3d0d91))



<a name="0.28.1"></a>
## [0.28.1](https://github.com/webpack/css-loader/compare/v0.28.0...v0.28.1) (2017-05-02)


### Bug Fixes

* allow to specify a full hostname as a root URL ([#521](https://github.com/webpack/css-loader/issues/521)) ([06d27a1](https://github.com/webpack/css-loader/commit/06d27a1))
* case insensitivity of [@import](https://github.com/import) ([#514](https://github.com/webpack/css-loader/issues/514)) ([de4356b](https://github.com/webpack/css-loader/commit/de4356b))
* don't handle empty [@import](https://github.com/import) and url() ([#513](https://github.com/webpack/css-loader/issues/513)) ([868fc94](https://github.com/webpack/css-loader/commit/868fc94))
* imported variables are replaced in exports if followed by a comma ([#504](https://github.com/webpack/css-loader/issues/504)) ([956bad7](https://github.com/webpack/css-loader/commit/956bad7))
* loader now correctly handles `url` with space(s) ([#495](https://github.com/webpack/css-loader/issues/495)) ([534ea55](https://github.com/webpack/css-loader/commit/534ea55))
* url with a trailing space is now handled correctly ([#494](https://github.com/webpack/css-loader/issues/494)) ([e1ec4f2](https://github.com/webpack/css-loader/commit/e1ec4f2))
* use `btoa` instead `Buffer` ([#501](https://github.com/webpack/css-loader/issues/501)) ([fbb0714](https://github.com/webpack/css-loader/commit/fbb0714))


### Performance Improvements

* generate source maps only when explicitly set ([#478](https://github.com/webpack/css-loader/issues/478)) ([b8f5c8f](https://github.com/webpack/css-loader/commit/b8f5c8f))



<a name="0.28.0"></a>
# [0.28.0](https://github.com/webpack/css-loader/compare/v0.27.3...v0.28.0) (2017-03-30)


### Features

* add alias feature to rewrite URLs ([#274](https://github.com/webpack/css-loader/issues/274)) ([c8db489](https://github.com/webpack/css-loader/commit/c8db489))



<a name="0.27.3"></a>
## [0.27.3](https://github.com/webpack/css-loader/compare/v0.27.2...v0.27.3) (2017-03-13)



<a name="0.27.2"></a>
# [0.27.2](https://github.com/webpack/css-loader/compare/v0.27.1...v0.27.2) (2017-03-12)

<a name="0.27.1"></a>
# [0.27.1](https://github.com/webpack/css-loader/compare/v0.27.0...v0.27.1) (2017-03-10)

<a name="0.27.0"></a>
# [0.27.0](https://github.com/webpack/css-loader/compare/v0.26.2...v0.27.0) (2017-03-10)


### Bug Fixes

* **sourcemaps:** use abs paths & remove sourceRoot ([c769ac3](https://github.com/webpack/css-loader/commit/c769ac3))
* `minimizeOptions` should be `query.minimize`! ([16c0858](https://github.com/webpack/css-loader/commit/16c0858))
* do not export duplicate keys ([#420](https://github.com/webpack/css-loader/issues/420)) ([a2b85d7](https://github.com/webpack/css-loader/commit/a2b85d7))


### Features

* allow removal of original class name ([#445](https://github.com/webpack/css-loader/issues/445)) ([3f78361](https://github.com/webpack/css-loader/commit/3f78361))
* Include the sourceMappingURL & sourceURL when toString() ([6da7e90](https://github.com/webpack/css-loader/commit/6da7e90))
