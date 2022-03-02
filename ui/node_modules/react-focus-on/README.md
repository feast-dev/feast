<div align="center">
  <h1>👁 React-Focus-On </h1>
  <br/>
   lock and loaded!
  <br/>
  
  <a href="https://www.npmjs.com/package/react-focus-on">
    <img src="https://img.shields.io/npm/v/react-focus-on.svg?style=flat-square" />
  </a>
    
  <a href="https://travis-ci.org/theKashey/react-focus-on">
   <img src="https://img.shields.io/travis/theKashey/react-focus-on.svg?style=flat-square" alt="Build status">
  </a> 

  <a href="https://www.npmjs.com/package/react-focus-on">
   <img src="https://img.shields.io/npm/dm/react-focus-on.svg" alt="npm downloads">
  </a> 

  <a href="https://bundlephobia.com/result?p=react-focus-on">
   <img src="https://img.shields.io/bundlephobia/minzip/react-focus-on.svg" alt="bundle size">
  </a>   
  <br/>
</div>

The final solution for WAI ARIA compatible Modal Dialogs or any full-screen tasks:
- locks __focus__ inside using [react-focus-lock](https://github.com/theKashey/react-focus-lock)
- disables page __scroll__ and user interactions using [react-remove-scroll](https://github.com/theKashey/react-remove-scroll)
- hides rest of a page from screen-readers using [aria-hidden](https://github.com/theKashey/aria-hidden)

Now you could __focus on__ a single task.

> This is basically the `inert` 

_Minimal_ size - __no more than 2kb__, _maximal_ - no more that 6kb. See sidecar example for details.

## Example
Code sandbox example - https://codesandbox.io/s/p3vjp8mzw7
```js
import {FocusOn} from 'react-focus-on';

<FocusOn
 onClickOutside={callback}
 onEscapeKey={callback}
 shards={[externalRef]}
>
 content you should be "focused" on
</FocusOn>
```

# API
### FocusOn
`FocusOn` - the focus on component
 - `enabled` - controls behaviour
 - `[shards]` - a list of Refs to be considered as a part of the Lock. A way to properly handle portals or scattered lock.
--- 
 - `[autoFocus=true]` - enables or disables `auto focus` management (see [react-focus-lock documentation](https://github.com/theKashey/react-focus-lock))
 - `[returnFocus=true]` - enables or disables `return focus` on lock deactivation (see [react-focus-lock documentation](https://github.com/theKashey/react-focus-lock))
 - `[whiteList=fn]` - you could whitelist locations FocusLock should carry about. Everything outside it will ignore. For example - any modals (see [react-focus-lock documentation](https://github.com/theKashey/react-focus-lock))
---
 - `[gapMode]` - the way removed ScrollBar would be _compensated_ - margin(default), or padding. See [scroll-locky documentation](https://github.com/theKashey/react-scroll-locky#gap-modes) to find the one you need.
 - `[noIsolation]` - disables aria-hidden isolation
 - `[inert]` - enables pointer-events isolation (☠️ dangerous, use to disable "parent scrollbars", refer to [react-remove-scroll](https://github.com/theKashey/react-remove-scroll) documentation)
 - `[allowPinchZoom]` - enables "pinch-n-zoom" behavior. By default it might be prevented, refer to [react-remove-scroll](https://github.com/theKashey/react-remove-scroll) documentation
---
 - `[onActivation]` - on activation callback
 - `[onDeactivation]` - on deactivation callback
---
 - `[onClickOutside]` - on click outside of "focus" area. (actually on any event "outside")
 - `[onEscapeKey]` - on Esc key down (and not defaultPrevented)
 
## Additional API
### Exposed from React-Focus-Lock
 - `AutoFocusInside` - to mark autofocusable element
 - `MoveFocusInside` - to move focus inside a component on mount
 - `InFocusGuard` - to "guard" a shard node (place an invisible node before and after)
 
See [react-focus-lock](https://github.com/theKashey/react-focus-lock) documentation for details.
 
### Exposed from React-Remove-Scroll
 - `classNames.fullWidth` - "100%" width (will not change on scrollbar removal)
 - `classNames.zeroRight` - "0" right (will not change on scrollbar removal)
  
See [react-remove-scroll](https://github.com/theKashey/react-remove-scroll) for details.

> PS: Version 1 used React-scroll-locky which was replaced by remove-scroll.

# Size
- (🧩 full) 5.7kb after compression (excluding tslib).
---
- (👁 UI) __2kb__, visual elements only
- (🚗 sidecar) 4kb, side effects  
  
### Import full
```js
import {FocusOn} from 'react-focus-on';

<FocusOn>
 {content}
</FocusOn> 
```  

### Import UI only
```js
import {FocusOn} from 'react-focus-on/UI';
import {sidecar} from "use-sidecar";

const FocusOnSidecar = sidecar(  
  () => import(/* webpackPrefetch: true */ "react-focus-on/sidecar")
);

<FocusOn
    sideCar={FocusOnSidecar}
>
 {content}
</FocusOn> 
```

# React versions
- v1 and v2 might work with React 15/16
- v3 require React 16.8+ (hooks)

# Licence
 MIT
  
