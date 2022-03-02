function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// THIS IS A GENERATED FILE. DO NOT MODIFY MANUALLY. @see scripts/compile-icons.js
import * as React from 'react';
import { jsx as ___EmotionJSX } from "@emotion/react";

var EuiIconEql = function EuiIconEql(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    d: "M15.862 14.18v.001a.482.482 0 01.1.521.5.5 0 01-.447.299h-3.149c-.07 0-.136 0-.2-.003a.664.664 0 01-.414-.14c-.35-.31-.684-.637-1.001-.981l-1.772-1.738a5.941 5.941 0 01-3.802.14l-.045-.012a5.89 5.89 0 01-2.682-1.712A5.715 5.715 0 011.08 7.72a5.66 5.66 0 01.348-3.118A5.78 5.78 0 013.39 2.124a5.979 5.979 0 016.136-.505 5.836 5.836 0 012.356 2.123 5.67 5.67 0 01.873 3.017l.001.003c0 .128-.051.25-.143.34a.495.495 0 01-.694 0 .476.476 0 01-.143-.34c0-.95-.288-1.878-.826-2.668a4.88 4.88 0 00-2.198-1.769 4.99 4.99 0 00-2.83-.273 4.93 4.93 0 00-2.509 1.314 4.774 4.774 0 00-1.34 2.46A4.715 4.715 0 002.352 8.6a4.82 4.82 0 001.804 2.155c.805.528 1.752.81 2.721.81a4.899 4.899 0 001.296-.194l.032-.009.717-.211a.506.506 0 01.483.111l1.11 1.026 1.788 1.752h2.03l-3.657-3.583a.475.475 0 01.014-.666.495.495 0 01.679-.013l4.493 4.402z"
  }), ___EmotionJSX("path", {
    d: "M7.097 3.468L9.679 4.93a.433.433 0 01.218.37v2.924a.422.422 0 01-.218.37l-2.582 1.461a.438.438 0 01-.437 0L4.077 8.594a.433.433 0 01-.218-.37V5.3a.422.422 0 01.218-.37L6.66 3.467a.446.446 0 01.437 0zm1.845 4.27V5.784a.328.328 0 00-.17-.287L7.047 4.52a.342.342 0 00-.338 0l-1.726.977a.336.336 0 00-.168.287v1.953a.326.326 0 00.17.287l1.724.977a.342.342 0 00.338 0l1.726-.978a.334.334 0 00.169-.287z"
  }));
};

export var icon = EuiIconEql;