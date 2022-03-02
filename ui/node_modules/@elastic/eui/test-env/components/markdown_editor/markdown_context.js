"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiMarkdownContext = void 0;

var _react = require("react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var EuiMarkdownContext = /*#__PURE__*/(0, _react.createContext)({
  openPluginEditor: function openPluginEditor() {},
  replaceNode: function replaceNode() {}
});
exports.EuiMarkdownContext = EuiMarkdownContext;