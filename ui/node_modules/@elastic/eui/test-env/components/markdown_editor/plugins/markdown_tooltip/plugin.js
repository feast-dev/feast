"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.tooltipPlugin = void 0;

var _react = _interopRequireDefault(require("react"));

var _code = require("../../../code");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var tooltipPlugin = {
  name: 'tooltipPlugin',
  button: {
    label: 'Tooltip',
    iconType: 'editorComment'
  },
  formatting: {
    prefix: '!{tooltip[',
    suffix: ']()}',
    trimFirst: true
  },
  helpText: (0, _react2.jsx)(_code.EuiCodeBlock, {
    language: "md",
    paddingSize: "s",
    fontSize: "l"
  }, '!{tooltip[anchor text](helpful description)}')
};
exports.tooltipPlugin = tooltipPlugin;