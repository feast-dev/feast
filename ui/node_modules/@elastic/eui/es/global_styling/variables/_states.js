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
import { computed } from '../../services/theme/utils';
import { sizeToPixel } from '../../services/theme/size';
import { shade, tint, transparentize } from '../../services/color';
export var focus = {
  color: computed(function (_ref) {
    var colors = _ref.colors;
    return transparentize(colors.primary, 0.3);
  }),
  transparency: {
    LIGHT: 0.1,
    DARK: 0.3
  },
  backgroundColor: {
    LIGHT: computed(function (_ref2) {
      var _ref3 = _slicedToArray(_ref2, 2),
          primary = _ref3[0],
          transparency = _ref3[1];

      return tint(primary, 1 - transparency);
    }, ['colors.primary', 'focus.transparency']),
    DARK: computed(function (_ref4) {
      var _ref5 = _slicedToArray(_ref4, 2),
          primary = _ref5[0],
          transparency = _ref5[1];

      return shade(primary, 1 - transparency);
    }, ['colors.primary', 'focus.transparency'])
  },
  // Sizing
  widthLarge: computed(sizeToPixel(0.25)),
  width: computed(sizeToPixel(0.125)),
  // Outline
  outline: {
    'box-shadow': computed(function (_ref6) {
      var _ref7 = _slicedToArray(_ref6, 2),
          color = _ref7[0],
          width = _ref7[1];

      return "0 0 0 ".concat(width, " ").concat(color);
    }, ['focus.color', 'focus.width'])
  }
};