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
export var border = {
  color: computed(function (_ref) {
    var _ref2 = _slicedToArray(_ref, 1),
        lightShade = _ref2[0];

    return lightShade;
  }, ['colors.lightShade']),
  width: {
    thin: '1px',
    thick: '2px'
  },
  radius: {
    medium: computed(sizeToPixel(0.25)),
    small: computed(sizeToPixel(0.125))
  },
  thin: computed(function (_ref3) {
    var _ref4 = _slicedToArray(_ref3, 2),
        width = _ref4[0],
        color = _ref4[1];

    return "".concat(width.thin, " solid ").concat(color);
  }, ['border.width', 'border.color']),
  thick: computed(function (_ref5) {
    var _ref6 = _slicedToArray(_ref5, 2),
        width = _ref6[0],
        color = _ref6[1];

    return "".concat(width.thick, " solid ").concat(color);
  }, ['border.width', 'border.color']),
  editable: computed(function (_ref7) {
    var _ref8 = _slicedToArray(_ref7, 2),
        width = _ref8[0],
        color = _ref8[1];

    return "".concat(width.thick, " dotted ").concat(color);
  }, ['border.width', 'border.color'])
};