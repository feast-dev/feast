# JavaScript-based theming in EUI

The style system to replace Sass and Sass-based design tokens in EUI.

* Theme construction via a Proxy-based system with cascading and computed values
  * Proxy-based: allows for the theme system to reference its own values (for reuse or for computational manipulation)
  * Cascading: conceptually similar to Sass, where variable location and order are important
  * Extendable: allows for appending style variables to the EUI theme structure, scoped to a React context provider
  * Override-able: all theme tokens/variables can be altered by consumers
* Theme consumption via React hook and HOC methods
* Color mode support as first-class consideration
  * "Light" and "dark" mode accounting
  * Theme consumption is scoped to the current color mode (set in the context provider)
* Style adaptaion based on a smal set of base values
  * Text colors are calculated with WCAG Level AA (4.5:1) in mind
  * Scalable typographic and spacing rhythms 


## Layers of the theme system

### Unbuilt theme

_See [`euiThemeDefault`](../../themes/eui/theme.ts)_
An unbuilt theme is a composed object of style values or `computed` functions.

#### Style values

Think design tokens or CSS property values. Ready to be consumed as-is in an application environment, using some JavaScript method of applying styles (i.e., a CSS-in-JS library is not required).

#### `computed` functions

These properties specify that the value depends upon some other value in the theme, in the shape of:

```js
computed(
  ([size]) => size * 2 // predicate. What to do with the dependency values,
  ['sizes.euiSize'], // dependency array, referencing other properties in the theme object
)
```

The dependency array is optional. Omitting the array gives access to the computed theme.

```js
computed(
  (theme) => theme.sizes.euiSize * 2
)
```

### Theme system (built theme)

_See [`EuiThemeDefault`](../../themes/eui/theme.ts)_
A built theme by way of `buildTheme`, which transforms the object containing static style values and `computed` functions into a JavaScript Proxy object with handler traps. In this state, the theme is essentially inaccessible and immutable, that is, it requires `getComputed` to correctly order and access values and dependencies, and `set()` is disabled.

### Computed theme

_See [`EuiThemeContext`](../../themes/eui/context.ts)_
A consumable theme object in which all `computed` function values have been computed; all values are accessible and usable in an application environment.
Returned from `getComputed`, in the shape of:

```js
getComputed(
  EuiThemeDefault, // Theme system (Proxy)
  {}, // Modifications object
  'light' // Color mode
)
```

#### Modifications

Because the theme system (built theme) is immutable, modifications can only be made at compute time by providing overrides and extensions for theme property values. These modifications are passed to the `EuiThemeProvider` via the `modify` prop and should match the high-level object shape of the theme. 

#### Color mode

Think light and dark mode. A theme has built-in color mode support, using the reserved `LIGHT` and `DARK` keys as a marker:

```js
colors: {
  LIGHT: {...}
  DARK : {...}
}
```
The reserved color mode keys can be used at any level and location in a theme.
`getComputed` will only compute and return values in the specified current color mode.


## React-specific context

### EuiThemeProvider

_See [`EuiThemeProvider`](../../themes/eui/provider.ts)_
Umbrella provider component that holds the various top-level theme configuration option providers: theme system, color mode, modifications; as well as the primary output provider: computed theme.
The actual computation for computed theme values takes place at this level, where the three inputs are known (theme system, color mode, modifications) and the output (computed theme) can be cached for consumption. Input changes are captured and the output is recomputed.

```js
<EuiThemeProvider
  theme={DefaultEuiTheme}
  colorMode="light"
  modify={{}}
  />
```

All three props are optional. The default values for EUI will be used in the event that no configuration is provided. Note, however that colorMode switching will require consumers to maintain that app state.

### useEuiTheme

_See [`useEuiTheme`](../../themes/eui/hooks.tsx)_
A custom React hook that returns the computed theme. This hook is little more than a wrapper around the `useContext` hook, accessing three of the top-level providers: computed theme, color mode, and modifications.

```js
const { euiTheme, colorMode, modifications } = useEuiTheme();
```

The `euiTheme` variable has TypeScript support, which will result in IDE autocomplete availability.

### WithEuiTheme
A higher-order-component that wraps `useEuiTheme` for React class components.


___


## Emotion

[Emotion](https://emotion.sh/docs/introduction) is the CSS-in-JS library currently selected for use in EUI. Nothing in the EUI theming system is dependent upon Emotion packages, but the Emotion ecosystem will have impacts on generated styles.

### Composition

* Prefer the use of [`css` prop](https://emotion.sh/docs/css-prop) construction over [styled-component-like](https://emotion.sh/docs/styled) component construction
* Babel-based build accommodation

### Testing

Snapshot testing ([as currently configured](https://emotion.sh/docs/testing#writing-a-test)) will result in generic `emotion-${n}` class names with the generated style object as part of the snapshot.

* This seems good for EUI, but it also affects consumers
  * Consumers will need to use the `@emotion/jest` snapshot serializer to avoid class name churn.
  * Not ideal; unsure of any other solutions
* During the conversion process, the snapshot diffs will look less than ideal when using `shallow` (a single wrapper element; DOM itself is unchanged):

```diff
-  <div
-    className="euiTableRowCell__mobileHeader euiTableRowCell--hideForDesktop"
+  <EmotionCssPropInternal
+    __EMOTION_LABEL_PLEASE_DO_NOT_USE__="EuiTableRowCell"
+    __EMOTION_TYPE_PLEASE_DO_NOT_USE__="td"
+    className="euiTableRowCell"
+    style={
+      Object {
+        "width": undefined,
+    }
+      }
    >
```
