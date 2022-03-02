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
import { jsx as ___EmotionJSX } from "@emotion/react";
var tableLayoutToClassMap = {
  fixed: null,
  auto: 'euiTable--auto'
};
export var EuiTable = function EuiTable(_ref) {
  var children = _ref.children,
      className = _ref.className,
      compressed = _ref.compressed,
      _ref$tableLayout = _ref.tableLayout,
      tableLayout = _ref$tableLayout === void 0 ? 'fixed' : _ref$tableLayout,
      _ref$responsive = _ref.responsive,
      responsive = _ref$responsive === void 0 ? true : _ref$responsive,
      rest = _objectWithoutProperties(_ref, ["children", "className", "compressed", "tableLayout", "responsive"]);

  var classes = classNames('euiTable', className, {
    'euiTable--compressed': compressed,
    'euiTable--responsive': responsive
  }, tableLayoutToClassMap[tableLayout]);
  return ___EmotionJSX("table", _extends({
    tabIndex: -1,
    className: classes
  }, rest), children);
};
EuiTable.propTypes = {
  compressed: PropTypes.bool,
  responsive: PropTypes.bool,

  /**
     * Sets the table-layout CSS property
     */
  tableLayout: PropTypes.oneOf(["fixed", "auto"]),
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};