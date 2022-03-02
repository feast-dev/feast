# Changes to PostCSS Lab Function

### 4.0.3 (January 2, 2022)

- Removed Sourcemaps from package tarball.
- Moved CLI to CLI Package. See [announcement](https://github.com/csstools/postcss-plugins/discussions/121).

### 4.0.2 (December 13, 2021)

- Changed: now uses `postcss-value-parser` for parsing.
- Updated: documentation
- Added: support for CSS variables with `preserve: true` option.
- Fixed: Hue values with units in `lch` functions are now correctly handled.
- Fixed: Rounding of values to match current browser behavior.

### 4.0.1 (November 18, 2021)

- Added: Safeguards against postcss-values-parser potentially throwing an error.
- Updated: postcss-value-parser to 6.0.1 (patch)

### 4.0.0 (September 17, 2021)

- Updated: Support for PostCS 8+ (major).
- Updated: Support for Node 12+ (major).

### 3.1.2 (April 25, 2020)

- Updated: Publish

### 3.1.1 (April 25, 2020)

- Updated: Using `walkType` to evade walker bug in `postcss-values-parser`

### 3.1.0 (April 25, 2020)

- Updated: `postcss-values-parser` to 3.2.0 (minor).

### 3.0.1 (April 12, 2020)

- Updated: Ownership moved to CSSTools.

### 3.0.0 (April 12, 2020)

- Updated: `postcss-values-parser` to 3.1.1 (major).
- Updated: Node support to 10.0.0 (major).
- Updated: Feature to use new percentage syntax.
- Removed: Support for the removed `gray()` function.

### 2.0.1 (September 18, 2018)

- Updated: PostCSS Values Parser 2.0.0

### 2.0.0 (September 17, 2018)

- Updated: Support for PostCSS 7+
- Updated: Support for Node 6+

### 1.1.0 (July 24, 2018)

- Added: Support for `gray(a / b)` as `lab(a 0 0 / b)`

### 1.0.1 (May 11, 2018)

- Fixed: Values beyond the acceptable 0-255 RGB range

### 1.0.0 (May 11, 2018)

- Initial version
