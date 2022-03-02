"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiThemeContext = exports.EuiColorModeContext = exports.EuiModificationsContext = exports.EuiSystemContext = void 0;

var _react = require("react");

var _theme = require("../../themes/amsterdam/theme");

var _utils = require("./utils");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var EuiSystemContext = /*#__PURE__*/(0, _react.createContext)(_theme.EuiThemeAmsterdam);
exports.EuiSystemContext = EuiSystemContext;
var EuiModificationsContext = /*#__PURE__*/(0, _react.createContext)({});
exports.EuiModificationsContext = EuiModificationsContext;
var EuiColorModeContext = /*#__PURE__*/(0, _react.createContext)(_utils.DEFAULT_COLOR_MODE);
exports.EuiColorModeContext = EuiColorModeContext;
var EuiThemeContext = /*#__PURE__*/(0, _react.createContext)((0, _utils.getComputed)(_theme.EuiThemeAmsterdam, {}, _utils.DEFAULT_COLOR_MODE));
exports.EuiThemeContext = EuiThemeContext;