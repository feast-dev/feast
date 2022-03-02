# rehype-raw

[![Build][build-badge]][build]
[![Coverage][coverage-badge]][coverage]
[![Downloads][downloads-badge]][downloads]
[![Size][size-badge]][size]
[![Sponsors][sponsors-badge]][collective]
[![Backers][backers-badge]][collective]
[![Chat][chat-badge]][chat]

[**rehype**][rehype] plugin to parse the tree again (and raw nodes).
Keeping positional info OK.  🙌

Tiny wrapper around [`hast-util-raw`][raw]

## Install

[npm][]:

```sh
npm install rehype-raw
```

## Use

Say we have the following Markdown file, `example.md`:

```markdown
<div class="note">

A mix of *Markdown* and <em>HTML</em>.

</div>
```

And our script, `example.js`, looks as follows:

```js
var vfile = require('to-vfile')
var report = require('vfile-reporter')
var unified = require('unified')
var markdown = require('remark-parse')
var remark2rehype = require('remark-rehype')
var doc = require('rehype-document')
var format = require('rehype-format')
var stringify = require('rehype-stringify')
var raw = require('rehype-raw')

unified()
  .use(markdown)
  .use(remark2rehype, {allowDangerousHtml: true})
  .use(raw)
  .use(doc, {title: '🙌'})
  .use(format)
  .use(stringify)
  .process(vfile.readSync('example.md'), function (err, file) {
    console.error(report(err || file))
    console.log(String(file))
  })
```

Now, running `node example` yields:

```html
example.md: no issues found
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>🙌</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
  </head>
  <body>
    <div class="note">
      <p>A mix of <em>Markdown</em> and <em>HTML</em>.</p>
    </div>
  </body>
</html>
```

## API

### `rehype().use(raw[, options])`

Parse the tree again, also parsing “raw” nodes (as exposed by
[`remark-rehype`][remark-rehype]).
`options` are passed to [hast-util-raw][raw].

###### Note

This project parses a [**hast**][hast] tree with embedded raw HTML.
This typically occurs because we’re coming from Markdown, often parsed by
[`remark-parse`][remark-parse].
Inside Markdown, HTML is a black box: Markdown doesn’t know what’s inside that
HTML.
So, when `rehype-raw` maps Markdown to HTML, it cannot understand raw embedded
HTML.

That’s where this project comes in.

But, Markdown is much terser than HTML, so it’s often preferred to use Markdown,
in HTML, inside Markdown.
As can be seen in the above example.

However, Markdown can only be mixed with HTML in some cases.
Take the following examples:

*   **Warning**: does not work:

    ```markdown
    <div class="note">
    A mix of *Markdown* and <em>HTML</em>.
    </div>
    ```

    …this is seen as one big block of HTML:

    ```html
    <div class="note">
    A mix of *Markdown* and <em>HTML</em>.
    <div>
    ```

*   This does work:

    ```markdown
    <div class="note">

    A mix of *Markdown* and <em>HTML</em>.

    </div>
    ```

    …it’s one block with the opening HTML tag, then a paragraph of Markdown, and
    another block with closing HTML tag.
    That’s because of the blank lines:

    ```html
    <div class="note">
    A mix of <em>Markdown</em> and <em>HTML</em>.
    <div>
    ```

*   This also works:

    ```markdown
    <span class="note">A mix of *Markdown* and <em>HTML</em>.</span>
    ```

    …Inline tags are parsed as separate tags, with Markdown in between:

    ```html
    <p><span class="note">A mix of <em>Markdown</em> and <em>HTML</em>.</span></p>
    ```

    This occurs if the tag name is not included in the list of [block][] tag
    names.

## Security

Improper use of `rehype-raw` can open you up to a
[cross-site scripting (XSS)][xss] attack.

Either do not combine this plugin with user content or use
[`rehype-sanitize`][sanitize].

## Contribute

See [`contributing.md`][contributing] in [`rehypejs/.github`][health] for ways
to get started.
See [`support.md`][support] for ways to get help.

This project has a [code of conduct][coc].
By interacting with this repository, organization, or community you agree to
abide by its terms.

## License

[MIT][license] © [Titus Wormer][author]

<!-- Definitions -->

[build-badge]: https://github.com/rehypejs/rehype-raw/workflows/main/badge.svg

[build]: https://github.com/rehypejs/rehype-raw/actions

[coverage-badge]: https://img.shields.io/codecov/c/github/rehypejs/rehype-raw.svg

[coverage]: https://codecov.io/github/rehypejs/rehype-raw

[downloads-badge]: https://img.shields.io/npm/dm/rehype-raw.svg

[downloads]: https://www.npmjs.com/package/rehype-raw

[size-badge]: https://img.shields.io/bundlephobia/minzip/rehype-raw.svg

[size]: https://bundlephobia.com/result?p=rehype-raw

[sponsors-badge]: https://opencollective.com/unified/sponsors/badge.svg

[backers-badge]: https://opencollective.com/unified/backers/badge.svg

[collective]: https://opencollective.com/unified

[chat-badge]: https://img.shields.io/badge/chat-discussions-success.svg

[chat]: https://github.com/rehypejs/rehype/discussions

[npm]: https://docs.npmjs.com/cli/install

[health]: https://github.com/rehypejs/.github

[contributing]: https://github.com/rehypejs/.github/blob/HEAD/contributing.md

[support]: https://github.com/rehypejs/.github/blob/HEAD/support.md

[coc]: https://github.com/rehypejs/.github/blob/HEAD/code-of-conduct.md

[license]: license

[author]: https://wooorm.com

[rehype]: https://github.com/rehypejs/rehype

[hast]: https://github.com/syntax-tree/hast

[raw]: https://github.com/syntax-tree/hast-util-raw

[remark-parse]: https://github.com/remarkjs/remark/blob/HEAD/packages/remark-parse

[remark-rehype]: https://github.com/remarkjs/remark-rehype

[block]: https://github.com/remarkjs/remark/blob/HEAD/packages/remark-parse/lib/block-elements.js

[xss]: https://en.wikipedia.org/wiki/Cross-site_scripting

[sanitize]: https://github.com/rehypejs/rehype-sanitize
