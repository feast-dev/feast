/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { hsvToRgb } from './hsv_to_rgb';
import { rgbToHex } from './rgb_to_hex';
export function hsvToHex(_ref) {
  var h = _ref.h,
      s = _ref.s,
      v = _ref.v;

  var _hsvToRgb = hsvToRgb({
    h: h,
    s: s,
    v: v
  }),
      r = _hsvToRgb.r,
      g = _hsvToRgb.g,
      b = _hsvToRgb.b;

  return rgbToHex("rgb(".concat(r, ", ").concat(g, ", ").concat(b, ")"));
}