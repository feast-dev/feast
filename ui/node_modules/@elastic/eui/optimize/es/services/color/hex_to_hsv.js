import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { hexToRgb } from './hex_to_rgb';
import { rgbToHsv } from './rgb_to_hsv';
export function hexToHsv(hex) {
  var _hexToRgb = hexToRgb(hex),
      _hexToRgb2 = _slicedToArray(_hexToRgb, 3),
      r = _hexToRgb2[0],
      g = _hexToRgb2[1],
      b = _hexToRgb2[2];

  return rgbToHsv({
    r: r,
    g: g,
    b: b
  });
}