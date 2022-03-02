# resolve-url-loader

## Version 4

**Features**

* Better resolution of the original source location - You can more successfully use `url()` in variables and mixins.
* Dependencies now accept a wider range and explicit dependency on `rework` and `rework-visit` has been removed.

**Breaking Changes**

* The `engine` option is deprecated which means the old `rework` engine is deprecated.
* The `keepQuery` behaviour is now the default, the `keepQuery` option has been removed.
* The `removeCR` option defaults to `true` when executing on Windows OS.
* The `absolute` option has been removed.
* The `join` option has changed.

**Migrating**

Remove the `engine` option if you are using it - the default "postcss" engine is much more reliable. The "rework" engine will still work for now but will be removed in the next major version.

Remove the `keepQuery` option if you are using it.

Remove the `absolute` option, webpack should work fine without it. If you have a specific need to rebase `url()` then you should use a separate loader.

If you use a custom `join` function then you will need to refactor it to the new API. Refer to the advanced usage documentation.

If you wish to still use `engine: "rework"` then note that `rework` and `rework-visit` packages are now `peerDependencies` that must be explicitly installed by you.

## Version 3

**Features**

* Use `postcss` parser by default. This is long overdue as the old `rework` parser doesn't cope with modern css.

* Lots of automated tests running actual webpack builds. If you have an interesting use-case let me know.

**Breaking Changes**

* Multiple options changed or deprecated.
* Removed file search "magic" in favour of `join` option.
* Errors always fail and are no longer swallowed.
* Processing absolute asset paths requires `root` option to be set.

**Migrating**

Initially set option `engine: 'rework'` for parity with your existing build. Once working you can remove this option **or** set `engine: 'postcss'` explicitly.

Retain `keepQuery` option if you are already using it.

The `root` option now has a different meaning. Previously it limited file search. Now it is the base path for absolute or root-relative URIs, consistent with `css-loader`. If you are already using it you can probably remove it.

If you build on Windows platform **and** your content contains absolute asset paths, then `css-loader` could fail. The `root` option here may fix the URIs before they get to `css-loader`. Try to leave it unspecified, otherwise (windows only) set to empty string `root: ''`.
