# rehype-react

[![Build][build-badge]][build]
[![Coverage][coverage-badge]][coverage]
[![Downloads][downloads-badge]][downloads]
[![Size][size-badge]][size]
[![Sponsors][sponsors-badge]][collective]
[![Backers][backers-badge]][collective]
[![Chat][chat-badge]][chat]

[**rehype**][rehype] plugin to transform to [**React**][react].

## Install

[npm][]:

```sh
npm install rehype-react
```

## Use

The following example shows how to create a Markdown input textarea, and
corresponding rendered HTML output.
The Markdown is processed to add a Table of Contents, highlight code blocks, and
to render GitHub mentions (and other cool GH features).

```js
import React from 'react'
import ReactDOM from 'react-dom'
import unified from 'unified'
import markdown from 'remark-parse'
import slug from 'remark-slug'
import toc from 'remark-toc'
import github from 'remark-github'
import remark2rehype from 'remark-rehype'
import highlight from 'rehype-highlight'
import rehype2react from 'rehype-react'

var processor = unified()
  .use(markdown)
  .use(slug)
  .use(toc)
  .use(github, {repository: 'rehypejs/rehype-react'})
  .use(remark2rehype)
  .use(highlight)
  .use(rehype2react, {createElement: React.createElement})

class App extends React.Component {
  constructor() {
    super()
    this.state = {text: '# Hello\n\n## Table of Contents\n\n## @rhysd'}
    this.onChange = this.onChange.bind(this)
  }

  onChange(ev) {
    this.setState({text: ev.target.value})
  }

  render() {
    return (
      <div>
        <textarea value={this.state.text} onChange={this.onChange} />
        <div id="preview">
          {processor.processSync(this.state.text).result}
        </div>
      </div>
    )
  }
}

ReactDOM.render(<App />, document.querySelector('#root'))
```

Yields (in `id="preview"`, on first render):

```html
<div><h1 id="hello">Hello</h1>
<h2 id="table-of-contents">Table of Contents</h2>
<ul>
<li><a href="#rhysd">@rhysd</a></li>
</ul>
<h2 id="rhysd"><a href="https://github.com/rhysd"><strong>@rhysd</strong></a></h2></div>
```

## API

### `origin.use(rehype2react[, options])`

[**rehype**][rehype] ([hast][]) plugin to transform to [**React**][react].

Typically, [**unified**][unified] compilers return `string`.
This compiler returns a `ReactElement`.
When using `.process` or `.processSync`, the value at `file.result` (or when
using `.stringify`, the return value), is a `ReactElement`.
When using TypeScript, cast the type on your side.

> ℹ️ In [`unified@9.0.0`][unified-9], the result of `.process` changed from
> ~~`file.contents`~~ to `file.result`.

##### `options`

###### `options.createElement`

How to create elements or components (`Function`).
You should typically pass `React.createElement`.

###### `options.Fragment`

Create fragments instead of an outer `<div>` if available (`symbol`).
You should typically pass `React.Fragment`.

###### `options.components`

Override default elements (such as `<a>`, `<p>`, etcetera) by passing an object
mapping tag names to components (`Object.<Component>`, default: `{}`).

For example, to use `<MyLink>` components instead of `<a>`, and `<MyParagraph>`
instead of `<p>`, so something like this:

```js
  // …
  .use(rehype2react, {
    createElement: React.createElement,
    components: {
      a: MyLink,
      p: MyParagraph
    }
  })
  // …
```

###### `options.prefix`

React key prefix (`string`, default: `'h-'`).

###### `options.passNode`

Pass the original hast node as `props.node` to custom React components
(`boolean`, default: `false`).

## Security

Use of `rehype-react` can open you up to a [cross-site scripting (XSS)][xss]
attack if the tree is unsafe.
Use [`rehype-sanitize`][sanitize] to make the tree safe.

## Related

*   [`remark-rehype`](https://github.com/remarkjs/remark-rehype)
    — Transform Markdown ([**mdast**][mdast]) to HTML ([**hast**][hast])
*   [`rehype-retext`](https://github.com/rehypejs/rehype-retext)
    — Transform HTML ([**hast**][hast]) to natural language ([**nlcst**][nlcst])
*   [`rehype-remark`](https://github.com/rehypejs/rehype-remark)
    — Transform HTML ([**hast**][hast]) to Markdown ([**mdast**][mdast])
*   [`rehype-sanitize`][sanitize]
    — Sanitize HTML

## Contribute

See [`contributing.md`][contributing] in [`rehypejs/.github`][health] for ways
to get started.
See [`support.md`][support] for ways to get help.

This project has a [code of conduct][coc].
By interacting with this repository, organization, or community you agree to
abide by its terms.

## License

[MIT][license] © [Titus Wormer][titus], modified by [Tom MacWright][tom],
[Mapbox][], and [rhysd][].

<!-- Definitions -->

[build-badge]: https://github.com/rehypejs/rehype-react/workflows/main/badge.svg

[build]: https://github.com/rehypejs/rehype-react/actions

[coverage-badge]: https://img.shields.io/codecov/c/github/rehypejs/rehype-react.svg

[coverage]: https://codecov.io/github/rehypejs/rehype-react

[downloads-badge]: https://img.shields.io/npm/dm/rehype-react.svg

[downloads]: https://www.npmjs.com/package/rehype-react

[size-badge]: https://img.shields.io/bundlephobia/minzip/rehype-react.svg

[size]: https://bundlephobia.com/result?p=rehype-react

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

[titus]: https://wooorm.com

[tom]: https://macwright.org

[mapbox]: https://www.mapbox.com

[rhysd]: https://rhysd.github.io

[unified]: https://github.com/unifiedjs/unified

[rehype]: https://github.com/rehypejs/rehype

[mdast]: https://github.com/syntax-tree/mdast

[hast]: https://github.com/syntax-tree/hast

[nlcst]: https://github.com/syntax-tree/nlcst

[react]: https://github.com/facebook/react

[xss]: https://en.wikipedia.org/wiki/Cross-site_scripting

[sanitize]: https://github.com/rehypejs/rehype-sanitize

[unified-9]: https://github.com/unifiedjs/unified/releases/tag/9.0.0
