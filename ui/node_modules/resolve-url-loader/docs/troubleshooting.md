# Troubleshooting

Webpack is difficult to configure simply because it is so powerful. If you face a problem it is important to raise it in the right place.

Possibly whatever problem you are facing is _not_ an issue with this loader, so please work this list before raising an issue.

**Working with a framework**

1. Check to see if that framework is still using an older version with the `rework` engine. This will not support modern CSS and is the source of most problems. Usually there is an existing issue raised in that framework and there may be workarounds there.
2. Hack the framework code in your `node_modules` to diagose the root cause.

**Creating your own webpack config**

1. Do the checklist at the top of the page - do you _need_ to use this loader?
2. Read and understand the detail on [how the loader works](how-it-works.md).
3. Check the known-issues below.
4. Use the `debug` option to see where the loader is looking for your assets.
5. Temporarily remove this loader and use non-relative asset paths to check if the problem is something else.
6. Check [stack overflow](http://stackoverflow.com/search?q=resolve-url-loader) for an answer.
7. Review [previous issues](/issues?utf8=%E2%9C%93&q=is%3Aissue) that may be similar.
8. Try to recreate the problem with a minimum breaking example project.

I'm happy this loader helps so many people. Open-source is provided as-is and I'm currently **not** [dogfooding](https://en.wikipedia.org/wiki/Eating_your_own_dog_food) this loader in my own work, so please try not project your frustrations. There are some really great people who follow this project who can help.

## Known issues

### Support for `image-set()`

Right now this loader only rewrites `url()` statements.

If you need other statements processed, such as `image-set()`, then please upvote [issue #119](issues/119).

### Absolute URIs

By "absolute URIs" we more correctly mean assets with root-relative URLs or absolute file paths. These paths are **not** processed unless a `root` is specified.

However any paths that _are_ processed will have windows back-slash converted to posix forward-slash. This can be useful since some webpack loaders can choke on windows paths. By using `root: ''` then `resolve-url-loader` effectively does nothing to absolute paths except change the windows backslash.

**💡 Protip** In **windows** if your downstream loaders are choking on windows paths using `root: ''` can help.

Also it be useful to process absolute URIs if you have a custom `join` function and want to process all the paths. Although this is perhaps better done with some separate `postcss` plugin.

### Windows line breaks

Normal windows linebreaks are `CRLF`. But sometimes libsass will output single `CR` characters.

This problem is specific to multiline declarations. Refer to the [libsass bug #2693](https://github.com/sass/libsass/issues/2693).

If you have _any_ such multiline declarations preceding `url()` statements it will fail your build.

Libsass doesn't consider these orphan `CR` to be newlines but `postcss` engine does.  The result being an offset in source-map line-numbers which crashes `resolve-url-loader`.

```
Module build failed: Error: resolve-url-loader: error processing CSS
  source-map information is not available at url() declaration
```

Some users find the node-sass `linefeed` option solves the problem.

**Solutions**
* Try the node-sass [linefeed](https://github.com/sass/node-sass#linefeed--v300) option by way of `sass-loader`.

**Work arounds**

* Enable `removeCR` option [here](../README.md#options) (enabled by default on Window OS).
* Remove linebreaks in declarations in your `.scss` sources.

**Diagnosis**
1. Run a stand-alone sass build `npx node-sass index.scss output.css`.
2. Use a hex editor to check line endings `Format-Hex output.css`.
3. Expect `0DOA` (or desired) line endings. Single `0D` confirms this problem.
