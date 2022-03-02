"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.defaultUiPlugins = exports.getDefaultEuiMarkdownUiPlugins = void 0;

var MarkdownTooltip = _interopRequireWildcard(require("../markdown_tooltip"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var getDefaultEuiMarkdownUiPlugins = function getDefaultEuiMarkdownUiPlugins() {
  var _ref = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      exclude = _ref.exclude;

  var excludeSet = new Set(exclude);
  var uiPlugins = [];
  if (!excludeSet.has('tooltip')) uiPlugins.push(MarkdownTooltip.plugin); // @ts-ignore __originatedFromEui is a custom property

  uiPlugins.__originatedFromEui = true;
  return uiPlugins;
};

exports.getDefaultEuiMarkdownUiPlugins = getDefaultEuiMarkdownUiPlugins;
var defaultUiPlugins = getDefaultEuiMarkdownUiPlugins();
exports.defaultUiPlugins = defaultUiPlugins;