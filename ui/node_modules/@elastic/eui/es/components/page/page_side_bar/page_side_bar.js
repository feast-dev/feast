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
import React from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { keysOf } from '../../common';
import { jsx as ___EmotionJSX } from "@emotion/react";
var paddingSizeToClassNameMap = {
  none: null,
  s: 'euiPageSideBar--paddingSmall',
  m: 'euiPageSideBar--paddingMedium',
  l: 'euiPageSideBar--paddingLarge'
};
export var PADDING_SIZES = keysOf(paddingSizeToClassNameMap);
export var EuiPageSideBar = function EuiPageSideBar(_ref) {
  var children = _ref.children,
      className = _ref.className,
      sticky = _ref.sticky,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'none' : _ref$paddingSize,
      rest = _objectWithoutProperties(_ref, ["children", "className", "sticky", "paddingSize"]);

  var classes = classNames('euiPageSideBar', paddingSizeToClassNameMap[paddingSize], {
    'euiPageSideBar--sticky': sticky
  }, className);
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), children);
};
EuiPageSideBar.propTypes = {
  /**
     * Adds `position: sticky` and affords for any fixed position headers
     */
  sticky: PropTypes.bool,

  /**
     * Adds padding around the children
     */
  paddingSize: PropTypes.any,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};