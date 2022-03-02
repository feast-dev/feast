function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

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
import React, { useState } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { calculateThumbPosition, EUI_THUMB_SIZE } from './utils';
import { jsx as ___EmotionJSX } from "@emotion/react";

var calculateOffset = function calculateOffset(position, value, bound) {
  var threshold = 30;
  var offset = value === bound ? 0 : EUI_THUMB_SIZE / 2;

  if (offset !== 0) {
    // Estimating offset by eye. Trying to account for range scaling at both ends.
    offset = position <= threshold ? offset + 1 / position * threshold : offset;
    offset = position >= 100 - threshold ? offset - 1 / (100 - position) * threshold : offset;
  }

  return offset;
};

export var LEVEL_COLORS = ['primary', 'success', 'warning', 'danger'];
export var EuiRangeLevels = function EuiRangeLevels(_ref) {
  var _ref$levels = _ref.levels,
      levels = _ref$levels === void 0 ? [] : _ref$levels,
      max = _ref.max,
      min = _ref.min,
      showTicks = _ref.showTicks,
      compressed = _ref.compressed;

  var _useState = useState(0),
      _useState2 = _slicedToArray(_useState, 2),
      trackWidth = _useState2[0],
      setTrackWidth = _useState2[1];

  var handleRef = function handleRef(node) {
    var _node$clientWidth;

    setTrackWidth((_node$clientWidth = node === null || node === void 0 ? void 0 : node.clientWidth) !== null && _node$clientWidth !== void 0 ? _node$clientWidth : 0);
  };

  var validateLevelIsInRange = function validateLevelIsInRange(level) {
    if (level.min < min) {
      throw new Error("The level min of ".concat(level.min, " is lower than the min value of ").concat(min, "."));
    }

    if (level.max > max) {
      throw new Error("The level max of ".concat(level.max, " is higher than the max value of ").concat(max, "."));
    }
  };

  var classes = classNames('euiRangeLevels', {
    'euiRangeLevels--hasTicks': showTicks,
    'euiRangeLevels--compressed': compressed
  });
  return ___EmotionJSX("div", {
    className: classes,
    ref: handleRef
  }, levels.map(function (level, index) {
    validateLevelIsInRange(level);

    var color = level.color,
        className = level.className,
        levelMin = level.min,
        levelMax = level.max,
        rest = _objectWithoutProperties(level, ["color", "className", "min", "max"]);

    var left = 0;
    var right = 0;
    var leftOffset = 0;
    var rightOffset = 0;

    if (trackWidth > 0) {
      left = levelMin === min ? 0 : calculateThumbPosition(levelMin, min, max, trackWidth);
      leftOffset = calculateOffset(left, levelMin, min);
      right = levelMax === max ? 100 : calculateThumbPosition(levelMax, min, max, trackWidth);
      rightOffset = calculateOffset(right, levelMax, max);
    }

    var isNamedColor = LEVEL_COLORS.includes(color);
    var styles = {
      left: "calc(".concat(left, "% + ").concat(leftOffset, "px)"),
      right: "calc(".concat(100 - right, "% - ").concat(rightOffset, "px)"),
      backgroundColor: !isNamedColor ? color : undefined
    };
    var levelClasses = classNames('euiRangeLevel', _defineProperty({
      'euiRangeLevel--customColor': !isNamedColor
    }, "euiRangeLevel--".concat(color), isNamedColor), className);
    return ___EmotionJSX("span", _extends({
      key: index,
      style: styles,
      className: levelClasses
    }, rest));
  }));
};
EuiRangeLevels.propTypes = {
  /**
     * An array of #EuiRangeLevel objects
     */
  levels: PropTypes.arrayOf(PropTypes.shape({
    min: PropTypes.number.isRequired,
    max: PropTypes.number.isRequired,

    /**
       * Accepts one of `["primary", "success", "warning", "danger"]` or a valid CSS color value.
       */
    color: PropTypes.oneOfType([PropTypes.oneOf(["primary", "success", "warning", "danger"]).isRequired, PropTypes.any.isRequired]).isRequired,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }).isRequired),
  max: PropTypes.number.isRequired,
  min: PropTypes.number.isRequired,
  showTicks: PropTypes.bool,
  compressed: PropTypes.bool
};