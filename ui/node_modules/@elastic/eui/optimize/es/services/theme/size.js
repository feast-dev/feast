import _typeof from "@babel/runtime/helpers/typeof";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * Calculates the `px` value based on a scale multiplier
 * @param scale - The font scale multiplier
 * *
 * @param themeOrBase - Theme base value
 * *
 * @returns string - Rem unit aligned to baseline
 */
export var sizeToPixel = function sizeToPixel() {
  var scale = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : 1;
  return function (themeOrBase) {
    var base = _typeof(themeOrBase) === 'object' ? themeOrBase.base : themeOrBase;
    return "".concat(base * scale, "px");
  };
};