"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiProvider = void 0;

var _react = _interopRequireDefault(require("react"));

var _react2 = require("@emotion/react");

var _global_styling = require("../../global_styling");

var _services = require("../../services");

var _themes = require("../../themes");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var EuiProvider = function EuiProvider(_ref) {
  var cache = _ref.cache,
      _ref$theme = _ref.theme,
      theme = _ref$theme === void 0 ? _themes.EuiThemeAmsterdam : _ref$theme,
      _ref$globalStyles = _ref.globalStyles,
      GlobalStyles = _ref$globalStyles === void 0 ? _global_styling.EuiGlobalStyles : _ref$globalStyles,
      colorMode = _ref.colorMode,
      modify = _ref.modify,
      children = _ref.children;
  return theme !== null && GlobalStyles !== false ? (0, _react2.jsx)(_services.EuiThemeProvider, {
    theme: theme,
    colorMode: colorMode,
    modify: modify
  }, cache ? (0, _react2.jsx)(_react2.CacheProvider, {
    value: cache
  }, (0, _react2.jsx)(GlobalStyles, null)) : (0, _react2.jsx)(GlobalStyles, null), children) : (0, _react2.jsx)(_services.EuiThemeProvider, {
    colorMode: colorMode
  }, children);
};

exports.EuiProvider = EuiProvider;