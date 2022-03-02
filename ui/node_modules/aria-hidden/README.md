aria-hidden
====
[![NPM](https://nodei.co/npm/aria-hidden.png?downloads=true&stars=true)](https://nodei.co/npm/aria-hidden/)

Hides from ARIA everything, except provided node.
Helps to isolate modal dialogs and focused task - the content will be not accessible using
accesible tools.

# API
Just call `hideOthers` with DOM-node you want to keep, and it will hide everything else.
targetNode could be placed anywhere - its siblings would be hidden, but its parents - not.
```js
import {hideOthers} from 'aria-hidden';

const undo = hideOthers(DOMnode);
// everything else is "aria-hidden"

undo();
// all changes undone
```

you also may limit the effect by providing top level node as a second paramiter
```js
 hideOthers(targetNode, parentNode);
 hideOthers(anotherNode, document.getElementById('app'));
 // parentNode defaults to document.body 
```

# Inspiration
Based on [smooth-ui](https://github.com/smooth-code/smooth-ui) modal dialogs.

# See also
- [inert](https://github.com/WICG/inert) - The HTML attribute/property to mark parts of the DOM tree as "inert".
- [react-focus-lock](https://github.com/theKashey/react-focus-lock) to lock Focus inside modal.
- [react-scroll-lock](https://github.com/theKashey/react-scroll-lock) to disable page scroll while modal is opened.

# Size 
Code is 30 lines long

# Licence 
MIT