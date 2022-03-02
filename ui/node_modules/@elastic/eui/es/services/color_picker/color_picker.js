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
import { useMemo, useState } from 'react';

var generateRandomColor = function generateRandomColor() {
  return (// https://www.paulirish.com/2009/random-hex-color-code-snippets/
    "#".concat(Math.floor(Math.random() * 16777215).toString(16))
  );
};

export var useColorStopsState = function useColorStopsState() {
  var useRandomColor = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;
  var initialColorStops = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : [{
    stop: 20,
    color: '#54B399'
  }, {
    stop: 50,
    color: '#D36086'
  }, {
    stop: 65,
    color: '#9170B8'
  }];

  var _useState = useState(generateRandomColor()),
      _useState2 = _slicedToArray(_useState, 2),
      addColor = _useState2[0],
      setAddColor = _useState2[1];

  var _useState3 = useState(initialColorStops),
      _useState4 = _slicedToArray(_useState3, 2),
      colorStops = _useState4[0],
      setColorStops = _useState4[1];

  var updateColorStops = function updateColorStops(colorStops) {
    setColorStops(colorStops);

    if (useRandomColor) {
      setAddColor(generateRandomColor());
    }
  };

  return [colorStops, updateColorStops, addColor];
};
export var useColorPickerState = function useColorPickerState() {
  var initialColor = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : '';

  var _useState5 = useState(initialColor),
      _useState6 = _slicedToArray(_useState5, 2),
      color = _useState6[0],
      setColorValue = _useState6[1];

  var _useState7 = useState(true),
      _useState8 = _slicedToArray(_useState7, 2),
      isValid = _useState8[0],
      setIsValid = _useState8[1];

  var setColor = function setColor(text, _ref) {
    var isValid = _ref.isValid;
    setColorValue(text);
    setIsValid(isValid);
  };

  var errors = useMemo(function () {
    var hasErrors = !isValid;
    return hasErrors ? ['Provide a valid color value'] : null;
  }, [isValid]);
  return [color, setColor, errors];
};