/** @license React v16.3.2
 * react-is.production.min.js
 *
 * Copyright (c) 2013-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
'use strict';(function(b,c){"object"===typeof exports&&"undefined"!==typeof module?c(exports):"function"===typeof define&&define.amd?define(["exports"],c):c(b.ReactIs={})})(this,function(b){function c(a){if("object"===typeof a&&null!==a){var b=a.$$typeof;switch(b){case m:switch(a=a.type,a){case e:case f:case g:return a;default:switch(a=a&&a.$$typeof,a){case h:case k:case l:return a;default:return b}}case n:return b}}}var d="function"===typeof Symbol&&Symbol["for"],m=d?Symbol["for"]("react.element"):
60103,n=d?Symbol["for"]("react.portal"):60106,f=d?Symbol["for"]("react.fragment"):60107,g=d?Symbol["for"]("react.strict_mode"):60108,l=d?Symbol["for"]("react.provider"):60109,h=d?Symbol["for"]("react.context"):60110,e=d?Symbol["for"]("react.async_mode"):60111,k=d?Symbol["for"]("react.forward_ref"):60112;b.typeOf=c;b.AsyncMode=e;b.ContextConsumer=h;b.ContextProvider=l;b.Element=m;b.ForwardRef=k;b.Fragment=f;b.Portal=n;b.StrictMode=g;b.isValidElementType=function(a){return"string"===typeof a||"function"===
typeof a||a===f||a===e||a===g||"object"===typeof a&&null!==a&&(a.$$typeof===l||a.$$typeof===h||a.$$typeof===k)};b.isAsyncMode=function(a){return c(a)===e};b.isContextConsumer=function(a){return c(a)===h};b.isContextProvider=function(a){return c(a)===l};b.isElement=function(a){return"object"===typeof a&&null!==a&&a.$$typeof===m};b.isForwardRef=function(a){return c(a)===k};b.isFragment=function(a){return c(a)===f};b.isPortal=function(a){return c(a)===n};b.isStrictMode=function(a){return c(a)===g};Object.defineProperty(b,
"__esModule",{value:!0})});
