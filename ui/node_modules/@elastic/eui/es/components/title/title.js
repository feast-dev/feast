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
import classNames from 'classnames';
import { keysOf } from '../common';
var titleSizeToClassNameMap = {
  xxxs: 'euiTitle--xxxsmall',
  xxs: 'euiTitle--xxsmall',
  xs: 'euiTitle--xsmall',
  s: 'euiTitle--small',
  m: 'euiTitle--medium',
  l: 'euiTitle--large'
};
export var TITLE_SIZES = keysOf(titleSizeToClassNameMap);
var textTransformToClassNameMap = {
  uppercase: 'euiTitle--uppercase'
};
export var TEXT_TRANSFORM = keysOf(textTransformToClassNameMap);
export var EuiTitle = function EuiTitle(_ref) {
  var _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      children = _ref.children,
      className = _ref.className,
      textTransform = _ref.textTransform,
      rest = _objectWithoutProperties(_ref, ["size", "children", "className", "textTransform"]);

  var classes = classNames('euiTitle', titleSizeToClassNameMap[size], textTransform ? textTransformToClassNameMap[textTransform] : undefined, className, children.props.className);

  var props = _objectSpread({
    className: classes
  }, rest);

  return /*#__PURE__*/React.cloneElement(children, props);
};
EuiTitle.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * ReactElement to render as this component's content
     */
  children: PropTypes.element.isRequired,
  size: PropTypes.oneOf(["xxxs", "xxs", "xs", "s", "m", "l"]),
  textTransform: PropTypes.oneOf(["uppercase"]),
  id: PropTypes.string
};