"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.DEFAULT_VISUALIZATION_COLOR = exports.VISUALIZATION_COLORS = void 0;

var _eui_palettes = require("../../services/color/eui_palettes");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// Array of color-blind safe colors to use in visualizations or other
// spots that need a large range of varied, qualitative colors.
var VISUALIZATION_COLORS = (0, _eui_palettes.euiPaletteColorBlind)();
exports.VISUALIZATION_COLORS = VISUALIZATION_COLORS;
var DEFAULT_VISUALIZATION_COLOR = VISUALIZATION_COLORS[1];
exports.DEFAULT_VISUALIZATION_COLOR = DEFAULT_VISUALIZATION_COLOR;