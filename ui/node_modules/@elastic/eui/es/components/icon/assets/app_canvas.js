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

var EuiIconAppCanvas = function EuiIconAppCanvas(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M7 17h2v7H7zM12 14h2v10h-2zM17 16h2v8h-2zM22 14h3v2h-3zM22 18h3v2h-3zM22 22h3v2h-3z"
  }), ___EmotionJSX("path", {
    d: "M30.73 24a6.47 6.47 0 01.45-2.19c.337-.9.52-1.85.54-2.81a8.55 8.55 0 00-.54-2.81 6.47 6.47 0 01-.45-2.19 9.2 9.2 0 01.62-2.49c.53-1.57 1.08-3.19.08-4.2-1-1.01-2.41-.44-3.52.05a5.59 5.59 0 01-2.09.64 5.3 5.3 0 01-.59 0L16 .28 6.77 8a5.3 5.3 0 01-.59 0 5.59 5.59 0 01-2.09-.65C3 6.87 1.6 6.25.57 7.31c-1.03 1.06-.45 2.63.08 4.2A9.2 9.2 0 011.27 14a6.47 6.47 0 01-.45 2.19A8.55 8.55 0 00.28 19c.02.96.203 1.91.54 2.81A6.47 6.47 0 011.27 24a9.2 9.2 0 01-.62 2.49c-.53 1.57-1.08 3.19-.08 4.2.353.38.852.59 1.37.58a5.67 5.67 0 002.15-.63A5.59 5.59 0 016.18 30a7.13 7.13 0 012.29.47 8 8 0 002.62.53 7.37 7.37 0 002.47-.51A7.14 7.14 0 0116 30a6.24 6.24 0 012.14.45 8 8 0 002.77.55 8.08 8.08 0 002.77-.55 6.24 6.24 0 012.14-.45 5.59 5.59 0 012.09.65c1.11.49 2.49 1.11 3.52.05 1.03-1.06.45-2.63-.08-4.2a9.2 9.2 0 01-.62-2.5zM21.17 7h-.26a8 8 0 00-2.77.55A6.24 6.24 0 0116 8a6.24 6.24 0 01-2.14-.45A8 8 0 0011.09 7h-.26L16 2.72 21.17 7zm8.89 22.27a4.42 4.42 0 01-1.34-.46 7.08 7.08 0 00-2.9-.82 8.14 8.14 0 00-2.78.55 6.13 6.13 0 01-2.13.45 6.24 6.24 0 01-2.14-.45A8 8 0 0016 28a9 9 0 00-3.08.6 5.74 5.74 0 01-1.83.4 6.36 6.36 0 01-2-.43A8.72 8.72 0 006.18 28a7.08 7.08 0 00-2.9.82 9.65 9.65 0 01-1.28.52 6.08 6.08 0 01.52-2.21c.403-1 .65-2.055.73-3.13a8.55 8.55 0 00-.54-2.81A6.47 6.47 0 012.27 19a6.47 6.47 0 01.44-2.19c.337-.9.52-1.85.54-2.81a10.48 10.48 0 00-.72-3.13 9 9 0 01-.59-2.16H2c.447.1.88.255 1.29.46a7.08 7.08 0 002.9.82A8.14 8.14 0 009 9.44 6.13 6.13 0 0111.09 9a6.13 6.13 0 012.13.45A8.14 8.14 0 0016 10a8.14 8.14 0 002.78-.55A6.13 6.13 0 0120.91 9a6.13 6.13 0 012.09.44 8.14 8.14 0 002.78.55 7.08 7.08 0 002.9-.82A9.65 9.65 0 0130 8.66a6.08 6.08 0 01-.52 2.21c-.403 1-.65 2.055-.73 3.13.02.96.203 1.91.54 2.81a6.47 6.47 0 01.44 2.19 6.47 6.47 0 01-.44 2.19 8.55 8.55 0 00-.54 2.81c.078 1.074.32 2.13.72 3.13a9 9 0 01.59 2.16v-.02z"
  }));
};

export var icon = EuiIconAppCanvas;