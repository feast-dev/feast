function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

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
import { jsx as ___EmotionJSX } from "@emotion/react";
export var GROW_SIZES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
export var EuiFlexItem = function EuiFlexItem(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$grow = _ref.grow,
      grow = _ref$grow === void 0 ? true : _ref$grow,
      _ref$component = _ref.component,
      Component = _ref$component === void 0 ? 'div' : _ref$component,
      rest = _objectWithoutProperties(_ref, ["children", "className", "grow", "component"]);

  validateGrowValue(grow);
  var classes = classNames('euiFlexItem', _defineProperty({
    'euiFlexItem--flexGrowZero': !grow
  }, "euiFlexItem--flexGrow".concat(grow), typeof grow === 'number' ? GROW_SIZES.indexOf(grow) >= 0 : undefined), className);
  return (// @ts-ignore difficult to verify `rest` applies to `Component`
    ___EmotionJSX(Component, _extends({
      className: classes
    }, rest), children)
  );
};
EuiFlexItem.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  grow: PropTypes.oneOf([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, true, false, null]),
  component: PropTypes.any
};

function validateGrowValue(value) {
  var validValues = [null, undefined, true, false].concat(GROW_SIZES);

  if (validValues.indexOf(value) === -1) {
    throw new Error("Prop `grow` passed to `EuiFlexItem` must be a boolean or an integer between 1 and 10, received `".concat(value, "`"));
  }
}