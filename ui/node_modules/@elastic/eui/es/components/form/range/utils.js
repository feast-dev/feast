/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
export var EUI_THUMB_SIZE = 16;
export var calculateThumbPosition = function calculateThumbPosition(value, min, max, width) {
  var thumbSize = arguments.length > 4 && arguments[4] !== undefined ? arguments[4] : EUI_THUMB_SIZE;
  // Calculate the left position based on value
  var decimal = (value - min) / (max - min); // Must be between 0-100%

  var valuePosition = decimal <= 1 ? decimal : 1;
  valuePosition = valuePosition >= 0 ? valuePosition : 0;
  var trackWidth = width !== null && width !== void 0 ? width : 0;
  var thumbToTrackRatio = thumbSize / trackWidth;
  var trackPositionScale = (1 - thumbToTrackRatio) * 100;
  return valuePosition * trackPositionScale;
};