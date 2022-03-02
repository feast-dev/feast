function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import PropTypes from "prop-types";
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
EuiColorPaletteDisplayGradient.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Array of color `strings` or an array of #ColorStop. The stops must be numbers in an ordered range.
     */
  palette: PropTypes.oneOfType([PropTypes.arrayOf(PropTypes.string.isRequired).isRequired, PropTypes.arrayOf(PropTypes.shape({
    stop: PropTypes.number.isRequired,
    color: PropTypes.string.isRequired
  }).isRequired).isRequired]).isRequired
};