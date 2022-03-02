"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.withEuiTheme = exports.useEuiTheme = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _react = _interopRequireWildcard(require("react"));

var _context = require("./context");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var useEuiTheme = function useEuiTheme() {
  var theme = (0, _react.useContext)(_context.EuiThemeContext);
  var colorMode = (0, _react.useContext)(_context.EuiColorModeContext);
  var modifications = (0, _react.useContext)(_context.EuiModificationsContext);
  return {
    euiTheme: theme,
    colorMode: colorMode,
    modifications: modifications
  };
};

exports.useEuiTheme = useEuiTheme;

var withEuiTheme = function withEuiTheme(Component) {
  var componentName = Component.displayName || Component.name || 'Component';

  var Render = function Render(props, ref) {
    var _useEuiTheme = useEuiTheme(),
        euiTheme = _useEuiTheme.euiTheme,
        colorMode = _useEuiTheme.colorMode,
        modifications = _useEuiTheme.modifications;

    return (0, _react2.jsx)(Component, (0, _extends2.default)({
      theme: {
        euiTheme: euiTheme,
        colorMode: colorMode,
        modifications: modifications
      },
      ref: ref
    }, props));
  };

  var WithEuiTheme = /*#__PURE__*/(0, _react.forwardRef)(Render);
  WithEuiTheme.displayName = "WithEuiTheme(".concat(componentName, ")");
  return WithEuiTheme;
};

exports.withEuiTheme = withEuiTheme;