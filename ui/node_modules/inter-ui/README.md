# The Inter Typeface (npm distribution)

The CSS and web font files to easily self-host the [Inter font family](https://rsms.me/inter/) created by [Rasmus Andersson](https://rsms.me).

This repository is just a means of more easily distributing the font. It tracks the  releases of the main [inter repository](https://github.com/rsms/inter) as best as I am able.

At present it only contains the `woff` and `woff2` formats. As a result it supports versions of Chrome 5; Safari 5.1; Firefox 3.6; Opera 11.5; IE 9; Android 4.4; iOS 5.1 and above.

SCSS files are also available for use with the Sass preprocessor. The [`font-display` property](https://developer.mozilla.org/en-US/docs/Web/CSS/@font-face/font-display) can be overridden by setting `$inter-font-display` to a valid `font-display` value *before* importing the desired `.scss` file.

## Quick start

```sh
npm install --save inter-ui
```

### SCSS

Add the following to your SCSS:

```scss
@use "~inter-ui/default" with (
  $inter-font-display: swap,
  $inter-font-path: '~inter-ui/Inter (web)'
);
@use "~inter-ui/variable" with (
  $inter-font-display: swap,
  $inter-font-path: '~inter-ui/Inter (web)'
);
@include default.all;
@include variable.all;

html { font-family: "Inter", "system-ui"; }

@supports (font-variation-settings: normal) {
  html { font-family: "Inter var", "system-ui"; }
}
```

Note that this `@use` syntax is [not currently supported in the node-sass or ruby sass implementations](https://sass-lang.com/documentation/at-rules/use). We recommend using the primary sass implementation [dart sass](https://github.com/sass/dart-sass).

### JS/CSS

We have pre-built CSS files that you can include directly (with `font-display` being `swap`).

Add the following to your script:

```js
import "inter-ui/inter.css";
// Or use one of those versions:
// import "inter-ui/inter-latin.css";
// import "inter-ui/inter-hinted.css";
// import "inter-ui/inter-hinted-latin.css";
```

Add the following to your stylesheet:

```css
html { font-family: "Inter", "system-ui"; }

@supports (font-variation-settings: normal) {
  html { font-family: "Inter var", "system-ui"; }
}
```

## Quirks

If you're using the Apache web server to serve the font files, you will probably
encounter a *500 Internal Server Error* by default. This is because the font files contain
`.var.` in their name, which causes Apache to interpret those files in a special way.

There are two ways to solve this:

Either [adapt its configuration](https://serverfault.com/questions/159152/apache-treating-files-with-var-in-their-names-as-type-maps)
to make it serve variable fonts as expected.

Alternatively you can copy/rename the font files removing the `.var` in their name and use the
SCSS variable `$inter-font-variable-suffix` to change the filename in the CSS:

```scss
@use "~inter-ui/variable" with (
  $inter-font-variable-suffix: ''
);
```

## Modular imports

To avoid having to import all "font faces". You can also use only some of them via SCSS.

If you only want 400 and 700 you can specify exactly this.
```scss
@use "~inter-ui/default" as inter-ui with (
	$inter-font-path: "~inter-ui/Inter (web latin)"
);
@include inter-ui.weight-400;
@include inter-ui.weight-700;
```

## Versions

There are several versions you can choose from.
To use them with the modules, just change the `$inter-font-path` to e.g. `Inter (web hinted)`
or use the other pre-built CSS files.

### Hinted vs Unhinted

As detailed in the main repo:

> Inter font files comes in two versions:
>
> 1. "unhinted" -- Without TrueType hints (the default)
> 2. "hinted" -- With TrueType hints
>
> The TrueType hints are used by ClearType on Windows machines where ClearType
is enabled. This usually changes the appearance of the fonts and can in some
cases increase the legibility of text.
>
> Additionally, hints are little computer programs that takes up considerable
disk space, meaning that font files with hints are larger than those without
hints. This might be a consideration when using web fonts.

* SCSS use: set `$inter-font-path` to `Inter (web hinted)` or `Inter (web hinted latin)`
* JS/CSS use: import `inter-ui/inter-hinted.css` or `inter-ui/inter-hinted-latin.css`

### Latin

If you only need support for the latin characters. Then you can use this version.
The normal `Inter (web)` version average filesize is between 150kb and 100kb,
the reduced latin version is on average 30kb per font.

This was generated using [glyphhanger](https://github.com/filamentgroup/glyphhanger). See `package.json` for the build script.

* SCSS use: set `$inter-font-path` to `Inter (web latin)` or `Inter (web hinted latin)`
* JS/CSS use: import `inter-ui/inter-latin.css` or `inter-ui/inter-hinted-latin.css`
