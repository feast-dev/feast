function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

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
import React from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { calculateThumbPosition, EUI_THUMB_SIZE } from './utils';
import { useInnerText } from '../../inner_text';
import { jsx as ___EmotionJSX } from "@emotion/react";

var EuiTickValue = function EuiTickValue(_ref) {
  var disabled = _ref.disabled,
      customTicks = _ref.ticks,
      min = _ref.min,
      max = _ref.max,
      value = _ref.value,
      onChange = _ref.onChange,
      percentageWidth = _ref.percentageWidth,
      tickValue = _ref.tickValue,
      ticksRef = _ref.ticksRef;
  var tickStyle = {};
  var tickObject = customTicks ? customTicks.find(function (o) {
    return o.value === tickValue;
  }) : {
    value: tickValue,
    label: tickValue
  };
  var isMinTick = (tickObject === null || tickObject === void 0 ? void 0 : tickObject.value) === min;
  var isMaxTick = (tickObject === null || tickObject === void 0 ? void 0 : tickObject.value) === max;
  var label = tickObject ? tickObject.label : tickValue; // Math worked out by trial and error
  // Shifts the label into the reserved margin of EuiRangeTrack

  var labelShiftVal = (isMinTick || isMaxTick) && label.length > 3 ? Math.min(label.length * 0.25, 1.25) : 0;

  if (isMaxTick && !!labelShiftVal) {
    tickStyle.right = '0%';
  } else {
    var _ticksRef$current$cli, _ticksRef$current;

    var trackWidth = (_ticksRef$current$cli = (_ticksRef$current = ticksRef.current) === null || _ticksRef$current === void 0 ? void 0 : _ticksRef$current.clientWidth) !== null && _ticksRef$current$cli !== void 0 ? _ticksRef$current$cli : 0;
    var position = calculateThumbPosition(tickValue, min, max, trackWidth);
    var thumbOffset = labelShiftVal ? 0 : EUI_THUMB_SIZE / 2;
    tickStyle.left = "calc(".concat(position, "% + ").concat(thumbOffset, "px)");
  }

  tickStyle.maxWidth = customTicks ? undefined : "".concat(percentageWidth, "%");
  var pseudoShift = {};

  if (labelShiftVal) {
    var labelShift = isMaxTick ? 'marginRight' : 'marginLeft';
    tickStyle[labelShift] = "-".concat(labelShiftVal, "em");
    pseudoShift[labelShift] = "calc(".concat(labelShiftVal, "em + 4px)"); // 4px derived from .euiRangeTicks left/right offset
  }

  var pseudoTick = tickObject && !!labelShiftVal && (isMinTick || isMaxTick);
  var tickClasses = classNames('euiRangeTick', {
    'euiRangeTick--selected': value === tickValue,
    'euiRangeTick--isCustom': customTicks,
    'euiRangeTick--isMin': labelShiftVal && isMinTick,
    'euiRangeTick--isMax': labelShiftVal && isMaxTick,
    'euiRangeTick--hasTickMark': pseudoTick
  });

  var _useInnerText = useInnerText(),
      _useInnerText2 = _slicedToArray(_useInnerText, 2),
      ref = _useInnerText2[0],
      innerText = _useInnerText2[1];

  return ___EmotionJSX("button", {
    type: "button",
    className: tickClasses,
    value: tickValue,
    disabled: disabled,
    onClick: onChange,
    style: tickStyle,
    tabIndex: -1,
    ref: ref,
    title: typeof label === 'string' ? label : innerText
  }, pseudoTick && ___EmotionJSX("span", {
    className: "euiRangeTick__pseudo",
    "aria-hidden": true,
    style: pseudoShift
  }), label);
};

EuiTickValue.propTypes = {
  ticks: PropTypes.arrayOf(PropTypes.shape({
    value: PropTypes.number.isRequired,
    label: PropTypes.node.isRequired
  }).isRequired),
  tickSequence: PropTypes.arrayOf(PropTypes.number.isRequired).isRequired,
  value: PropTypes.oneOfType([PropTypes.number.isRequired, PropTypes.string.isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired]).isRequired).isRequired]),
  min: PropTypes.number.isRequired,
  max: PropTypes.number.isRequired,
  compressed: PropTypes.bool,
  interval: PropTypes.number,
  disabled: PropTypes.bool,
  onChange: PropTypes.func,
  ticksRef: PropTypes.any.isRequired,
  tickValue: PropTypes.any.isRequired,
  percentageWidth: PropTypes.number.isRequired
};
export var EuiRangeTicks = function EuiRangeTicks(props) {
  var ticks = props.ticks,
      tickSequence = props.tickSequence,
      max = props.max,
      min = props.min,
      _props$interval = props.interval,
      interval = _props$interval === void 0 ? 1 : _props$interval,
      compressed = props.compressed;
  var ticksRef = React.useRef(null); // Calculate the width of each tick mark

  var percentageWidth = interval / (max - min + interval) * 100;
  var classes = classNames('euiRangeTicks', {
    'euiRangeTicks--compressed': compressed,
    'euiRangeTicks--isCustom': ticks
  });
  return ___EmotionJSX("div", {
    className: classes,
    ref: ticksRef
  }, tickSequence.map(function (tickValue) {
    return ___EmotionJSX(EuiTickValue, _extends({
      key: tickValue
    }, props, {
      percentageWidth: percentageWidth,
      tickValue: tickValue,
      ticksRef: ticksRef
    }));
  }));
};
EuiRangeTicks.propTypes = {
  ticks: PropTypes.arrayOf(PropTypes.shape({
    value: PropTypes.number.isRequired,
    label: PropTypes.node.isRequired
  }).isRequired),
  tickSequence: PropTypes.arrayOf(PropTypes.number.isRequired).isRequired,
  value: PropTypes.oneOfType([PropTypes.number.isRequired, PropTypes.string.isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired]).isRequired).isRequired]),
  min: PropTypes.number.isRequired,
  max: PropTypes.number.isRequired,
  compressed: PropTypes.bool,
  interval: PropTypes.number,
  disabled: PropTypes.bool,
  onChange: PropTypes.func
};