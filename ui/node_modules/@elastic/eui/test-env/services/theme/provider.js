"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiThemeProvider = void 0;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _react = _interopRequireWildcard(require("react"));

var _isEqual = _interopRequireDefault(require("lodash/isEqual"));

var _context = require("./context");

var _utils = require("./utils");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var EuiThemeProvider = function EuiThemeProvider(_ref) {
  var _system = _ref.theme,
      _colorMode = _ref.colorMode,
      _modifications = _ref.modify,
      children = _ref.children;
  var parentSystem = (0, _react.useContext)(_context.EuiSystemContext);
  var parentModifications = (0, _react.useContext)(_context.EuiModificationsContext);
  var parentColorMode = (0, _react.useContext)(_context.EuiColorModeContext);
  var parentTheme = (0, _react.useContext)(_context.EuiThemeContext);

  var _useState = (0, _react.useState)(_system || parentSystem),
      _useState2 = (0, _slicedToArray2.default)(_useState, 2),
      system = _useState2[0],
      setSystem = _useState2[1];

  var prevSystemKey = (0, _react.useRef)(system.key);

  var _useState3 = (0, _react.useState)((0, _utils.mergeDeep)(parentModifications, _modifications)),
      _useState4 = (0, _slicedToArray2.default)(_useState3, 2),
      modifications = _useState4[0],
      setModifications = _useState4[1];

  var prevModifications = (0, _react.useRef)(modifications);

  var _useState5 = (0, _react.useState)((0, _utils.getColorMode)(_colorMode, parentColorMode)),
      _useState6 = (0, _slicedToArray2.default)(_useState5, 2),
      colorMode = _useState6[0],
      setColorMode = _useState6[1];

  var prevColorMode = (0, _react.useRef)(colorMode);
  var isParentTheme = (0, _react.useRef)(prevSystemKey.current === parentSystem.key && colorMode === parentColorMode && (0, _isEqual.default)(parentModifications, modifications));

  var _useState7 = (0, _react.useState)(isParentTheme.current && Object.keys(parentTheme).length ? parentTheme : (0, _utils.getComputed)(system, (0, _utils.buildTheme)(modifications, "_".concat(system.key)), colorMode)),
      _useState8 = (0, _slicedToArray2.default)(_useState7, 2),
      theme = _useState8[0],
      setTheme = _useState8[1];

  (0, _react.useEffect)(function () {
    var newSystem = _system || parentSystem;

    if (prevSystemKey.current !== newSystem.key) {
      setSystem(newSystem);
      prevSystemKey.current = newSystem.key;
      isParentTheme.current = false;
    }
  }, [_system, parentSystem]);
  (0, _react.useEffect)(function () {
    var newModifications = (0, _utils.mergeDeep)(parentModifications, _modifications);

    if (!(0, _isEqual.default)(prevModifications.current, newModifications)) {
      setModifications(newModifications);
      prevModifications.current = newModifications;
      isParentTheme.current = false;
    }
  }, [_modifications, parentModifications]);
  (0, _react.useEffect)(function () {
    var newColorMode = (0, _utils.getColorMode)(_colorMode, parentColorMode);

    if (!(0, _isEqual.default)(newColorMode, prevColorMode.current)) {
      setColorMode(newColorMode);
      prevColorMode.current = newColorMode;
      isParentTheme.current = false;
    }
  }, [_colorMode, parentColorMode]);
  (0, _react.useEffect)(function () {
    if (!isParentTheme.current) {
      setTheme((0, _utils.getComputed)(system, (0, _utils.buildTheme)(modifications, "_".concat(system.key)), colorMode));
    }
  }, [colorMode, system, modifications]);
  return (0, _react2.jsx)(_context.EuiColorModeContext.Provider, {
    value: colorMode
  }, (0, _react2.jsx)(_context.EuiSystemContext.Provider, {
    value: system
  }, (0, _react2.jsx)(_context.EuiModificationsContext.Provider, {
    value: modifications
  }, (0, _react2.jsx)(_context.EuiThemeContext.Provider, {
    value: theme
  }, children))));
};

exports.EuiThemeProvider = EuiThemeProvider;