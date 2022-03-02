"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.isDefaultTheme = exports.isLegacyTheme = exports.EUI_THEMES = void 0;

var _theme = require("./amsterdam/theme");

var _theme2 = require("./legacy/theme");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var EUI_THEMES = [{
  text: 'Light',
  value: 'light',
  provider: _theme.EuiThemeAmsterdam
}, {
  text: 'Dark',
  value: 'dark',
  provider: _theme.EuiThemeAmsterdam
}, {
  text: 'Legacy: Light',
  value: "".concat(_theme2.LEGACY_NAME_KEY, "_light"),
  provider: _theme2.EuiThemeLegacy
}, {
  text: 'Legacy: Dark',
  value: "".concat(_theme2.LEGACY_NAME_KEY, "_dark"),
  provider: _theme2.EuiThemeLegacy
}];
exports.EUI_THEMES = EUI_THEMES;

var isLegacyTheme = function isLegacyTheme(name) {
  return name === _theme2.LEGACY_NAME_KEY;
};

exports.isLegacyTheme = isLegacyTheme;

var isDefaultTheme = function isDefaultTheme(name) {
  return name === _theme.AMSTERDAM_NAME_KEY;
};

exports.isDefaultTheme = isDefaultTheme;