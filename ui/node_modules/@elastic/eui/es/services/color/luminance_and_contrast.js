/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
export function calculateLuminance(r, g, b) {
  var a = [r, g, b].map(function (v) {
    v /= 255;
    return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
  });
  return a[0] * 0.2126 + a[1] * 0.7152 + a[2] * 0.0722;
}
export function calculateContrast(rgb1, rgb2) {
  var contrast = (calculateLuminance(rgb1[0], rgb1[1], rgb1[2]) + 0.05) / (calculateLuminance(rgb2[0], rgb2[1], rgb2[2]) + 0.05);

  if (contrast < 1) {
    contrast = 1 / contrast;
  }

  return contrast;
}