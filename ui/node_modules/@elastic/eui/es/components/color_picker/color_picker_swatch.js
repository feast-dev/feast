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
import React, { forwardRef, useMemo } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { getChromaColor } from './utils';
import { useEuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiColorPickerSwatch = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var className = _ref.className,
      color = _ref.color,
      style = _ref.style,
      rest = _objectWithoutProperties(_ref, ["className", "color", "style"]);

  var classes = classNames('euiColorPickerSwatch', className);
  var chromaColor = useMemo(function () {
    return getChromaColor(color, true);
  }, [color]);
  var background = useMemo(function () {
    return chromaColor ? chromaColor.css() : 'transparent';
  }, [chromaColor]);
  var ariaLabel = useEuiI18n('euiColorPickerSwatch.ariaLabel', 'Select {color} as the color', {
    color: color
  });
  return ___EmotionJSX("button", _extends({
    type: "button",
    className: classes,
    "aria-label": ariaLabel,
    ref: ref,
    style: _objectSpread({
      background: background
    }, style)
  }, rest));
});
EuiColorPickerSwatch.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  color: PropTypes.string
};
EuiColorPickerSwatch.displayName = 'EuiColorPickerSwatch';