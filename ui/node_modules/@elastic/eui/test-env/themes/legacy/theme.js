"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiThemeLegacy = exports.euiThemeLegacy = exports.LEGACY_NAME_KEY = void 0;

var _utils = require("../../services/theme/utils");

var _animations = require("../../global_styling/variables/_animations");

var _breakpoint = require("../../global_styling/variables/_breakpoint");

var _colors = require("../../global_styling/variables/_colors");

var _size = require("../../global_styling/variables/_size");

var _typography = require("../../global_styling/variables/_typography");

var _borders = require("../../global_styling/variables/_borders");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var LEGACY_NAME_KEY = 'EUI_THEME_LEGACY';
exports.LEGACY_NAME_KEY = LEGACY_NAME_KEY;
var euiThemeLegacy = {
  colors: _colors.colors,
  base: _size.base,
  size: _size.size,
  font: _typography.font,
  border: _borders.border,
  animation: _animations.animation,
  breakpoint: _breakpoint.breakpoint
};
exports.euiThemeLegacy = euiThemeLegacy;
var EuiThemeLegacy = (0, _utils.buildTheme)(euiThemeLegacy, LEGACY_NAME_KEY);
exports.EuiThemeLegacy = EuiThemeLegacy;