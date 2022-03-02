# PostCSS Nesting [<img src="https://postcss.github.io/postcss/logo.svg" alt="PostCSS" width="90" height="90" align="right">][postcss]

[![NPM Version][npm-img]][npm-url]
[![CSS Standard Status][css-img]][css-url]
[![Support Chat][git-img]][git-url]

[PostCSS Nesting] lets you nest style rules inside each other, following the
[CSS Nesting] specification. If you want nested rules the same way [Sass] works
you might want to use [PostCSS Nested] instead.

```pcss
a, b {
  color: red;

  & c, & d {
    color: white;
  }
}

/* becomes */

a, b {
  color: red;
}

a c, a d, b c, b d {
  color: white;
}
```

## Usage

Add [PostCSS Nesting] to your project:

```bash
npm install postcss-nesting --save-dev
```

Use [PostCSS Nesting] to process your CSS:

```js
import postcssNesting from 'postcss-nesting';

postcssNesting.process(YOUR_CSS /*, processOptions, pluginOptions */);
```

Or use it as a [PostCSS] plugin:

```js
import postcss from 'postcss';
import postcssNesting from 'postcss-nesting';

postcss([
  postcssNesting(/* pluginOptions */)
]).process(YOUR_CSS /*, processOptions */);
```

[PostCSS Nesting] runs in all Node environments, with special instructions for:

| [Node](INSTALL.md#node) | [Webpack](INSTALL.md#webpack) | [Create React App](INSTALL.md#create-react-app) | [Gulp](INSTALL.md#gulp) | [Grunt](INSTALL.md#grunt) |
| --- | --- | --- | --- | --- |

### Deno

You can also use [PostCSS Nesting] on [Deno]:

```js
import postcss from "https://deno.land/x/postcss/mod.js";
import postcssNesting from "https://cdn.jsdelivr.net/npm/postcss-nesting@10/mod.js";

await postcss([postcssNesting]).process(YOUR_CSS /*, processOptions */);
```

## Options

### noIsPseudoSelector

#### Specificity

Before :

```css
#alpha,
.beta {
	&:hover {
		order: 1;
	}
}
```

After **without** the option :

```js
postcssNesting()
```

```css
:is(#alpha,.beta):hover {
	order: 1;
}
```

_`.beta:hover` has specificity as if `.beta` where an id selector, matching the specification._

[specificity: 1, 1, 0](https://polypane.app/css-specificity-calculator/#selector=%3Ais(%23alpha%2C.beta)%3Ahover)

After **with** the option :

```js
postcssNesting({
	noIsPseudoSelector: true
})
```

```css
#alpha:hover, .beta:hover {
	order: 1;
}
```

_`.beta:hover` has specificity as if `.beta` where a class selector, conflicting with the specification._

[specificity: 0, 2, 0](https://polypane.app/css-specificity-calculator/#selector=.beta%3Ahover)


#### Complex selectors

Before :

```css
.alpha > .beta {
	& + & {
		order: 2;
	}
}
```

After **without** the option :

```js
postcssNesting()
```

```css
:is(.alpha > .beta) + :is(.alpha > .beta) {
	order: 2;
}
```

After **with** the option :

```js
postcssNesting({
	noIsPseudoSelector: true
})
```

```css
.alpha > .beta + .alpha > .beta {
	order: 2;
}
```

_this is a different selector than expected as `.beta + .alpha` matches `.beta` followed by `.alpha`._<br>
_avoid these cases when you disable `:is()`_<br>
_writing the selector without nesting is advised here_

```css
/* without nesting */
.alpha > .beta + .beta {
	order: 2;
}
```

### ⚠️ Spec disclaimer

The [CSS Nesting Module] spec states on nesting that "Declarations occuring after a nested rule are invalid and ignored.".
While we think it makes sense on browsers, enforcing this at the plugin level introduces several constrains that would
interfere with PostCSS' plugin nature such as with `@mixin`

[css-img]: https://cssdb.org/badge/nesting-rules.svg
[css-url]: https://cssdb.org/#nesting-rules
[git-img]: https://img.shields.io/badge/support-chat-blue.svg
[git-url]: https://gitter.im/postcss/postcss
[npm-img]: https://img.shields.io/npm/v/postcss-nesting.svg
[npm-url]: https://www.npmjs.com/package/postcss-nesting

[CSS Nesting]: https://drafts.csswg.org/css-nesting-1/
[PostCSS]: https://github.com/postcss/postcss
[PostCSS Nesting]: https://github.com/csstools/postcss-plugins/tree/main/plugins/postcss-nesting
[Deno]: https://deno.land/x/postcss_nesting
[PostCSS Nested]: https://github.com/postcss/postcss-nested
[Sass]: https://sass-lang.com/
[CSS Nesting Module]: https://www.w3.org/TR/css-nesting-1/
