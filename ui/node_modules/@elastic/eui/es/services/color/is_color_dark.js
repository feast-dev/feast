function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * This function calculates if the specified color is "dark", which usually means
 * you need light text if you use it as a background color to fulfill WCAG contrast
 * requirement.
 * The color must be specified via its red, green and blue value in the range of
 * 0 to 255.
 * The formula is based on this Stackoverflow answer: https://stackoverflow.com/a/3943023
 * which itself is based upon the WCAG recommendation for color contrast.
 *
 * @param {number} red The red component in the range 0 to 255
 * @param {number} green The green component in the range 0 to 255
 * @param {number} blue The blue component in the range 0 to 255
 * @returns {boolean} True if the color is dark, false otherwise.
 */
export function isColorDark(red, green, blue) {
  var _map$map = [red, green, blue].map(function (c) {
    return c / 255.0;
  }).map(function (c) {
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  }),
      _map$map2 = _slicedToArray(_map$map, 3),
      r = _map$map2[0],
      g = _map$map2[1],
      b = _map$map2[2];

  var luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b;
  return luminance <= 0.179;
}