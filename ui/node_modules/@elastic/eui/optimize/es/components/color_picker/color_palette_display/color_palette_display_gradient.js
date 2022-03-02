import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { getLinearGradient } from '../utils';
import { EuiScreenReaderOnly } from '../../accessibility/screen_reader';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiColorPaletteDisplayGradient = function EuiColorPaletteDisplayGradient(_ref) {
  var palette = _ref.palette,
      title = _ref.title,
      _ref$style = _ref.style,
      style = _ref$style === void 0 ? {} : _ref$style,
      rest = _objectWithoutProperties(_ref, ["palette", "title", "style"]);

  var gradient = getLinearGradient(palette);
  return ___EmotionJSX(React.Fragment, null, title && ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, title)), ___EmotionJSX("span", _extends({
    // aria-hidden="true" is to ensure color blocks are ignored by screen readers,
    // and the only accessible text for options is the EuiScreenReaderOnly {title}
    "aria-hidden": "true",
    style: _objectSpread(_objectSpread({}, style), {}, {
      background: gradient
    })
  }, rest)));
};