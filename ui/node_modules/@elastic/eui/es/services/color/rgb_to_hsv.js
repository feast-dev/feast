/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
export function rgbToHsv(_ref) {
  var r = _ref.r,
      g = _ref.g,
      b = _ref.b;
  r /= 255;
  g /= 255;
  b /= 255;
  var max = Math.max(r, g, b);
  var min = Math.min(r, g, b);
  var delta = max - min;
  var hue;
  var value = max;
  var saturation = max === 0 ? 0 : delta / max;

  switch (max) {
    case min:
    default:
      hue = 0;
      break;

    case r:
      hue = (g - b) / delta + (g < b ? 6 : 0);
      break;

    case g:
      hue = (b - r) / delta + 2;
      break;

    case b:
      hue = (r - g) / delta + 4;
      break;
  }

  return {
    h: hue * 60,
    s: saturation,
    v: value
  };
}