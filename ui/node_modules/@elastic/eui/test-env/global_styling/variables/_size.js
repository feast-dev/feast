"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.size = exports.base = void 0;

var _utils = require("../../services/theme/utils");

var _size = require("../../services/theme/size");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var base = 16;
exports.base = base;
var size = {
  xxs: (0, _utils.computed)((0, _size.sizeToPixel)(0.125)),
  xs: (0, _utils.computed)((0, _size.sizeToPixel)(0.25)),
  s: (0, _utils.computed)((0, _size.sizeToPixel)(0.5)),
  m: (0, _utils.computed)((0, _size.sizeToPixel)(0.75)),
  base: (0, _utils.computed)((0, _size.sizeToPixel)()),
  l: (0, _utils.computed)((0, _size.sizeToPixel)(1.5)),
  xl: (0, _utils.computed)((0, _size.sizeToPixel)(2)),
  xxl: (0, _utils.computed)((0, _size.sizeToPixel)(2.5)),
  xxxl: (0, _utils.computed)((0, _size.sizeToPixel)(3)),
  xxxxl: (0, _utils.computed)((0, _size.sizeToPixel)(4))
};
exports.size = size;