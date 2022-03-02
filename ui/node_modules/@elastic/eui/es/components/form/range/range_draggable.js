function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

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
import { useMouseMove } from '../../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiRangeDraggable = function EuiRangeDraggable(_ref) {
  var className = _ref.className,
      showTicks = _ref.showTicks,
      lowerPosition = _ref.lowerPosition,
      upperPosition = _ref.upperPosition,
      compressed = _ref.compressed,
      onChange = _ref.onChange,
      min = _ref.min,
      max = _ref.max,
      disabled = _ref.disabled,
      value = _ref.value,
      rest = _objectWithoutProperties(_ref, ["className", "showTicks", "lowerPosition", "upperPosition", "compressed", "onChange", "min", "max", "disabled", "value"]);

  var outerStyle = {
    left: "calc(".concat(lowerPosition, ")"),
    right: "calc(100% - ".concat(upperPosition, " - 16px)")
  };
  var classes = classNames('euiRangeDraggable', {
    'euiRangeDraggable--hasTicks': showTicks,
    'euiRangeDraggable--compressed': compressed,
    'euiRangeDraggable--disabled': disabled
  }, className);

  var handleChange = function handleChange(_ref2, isFirstInteraction) {
    var x = _ref2.x;
    if (disabled) return;
    onChange(x, isFirstInteraction);
  };

  var _useMouseMove = useMouseMove(handleChange),
      _useMouseMove2 = _slicedToArray(_useMouseMove, 2),
      handleMouseDown = _useMouseMove2[0],
      handleInteraction = _useMouseMove2[1];

  var commonProps = {
    className: classes,
    role: 'slider',
    'aria-valuemin': min,
    'aria-valuemax': max,
    'aria-valuenow': value[0],
    'aria-valuetext': "".concat(value[0], ", ").concat(value[1]),
    'aria-disabled': !!disabled,
    tabIndex: !!disabled ? -1 : 0
  };
  return ___EmotionJSX("div", _extends({
    style: outerStyle
  }, commonProps, rest), ___EmotionJSX("div", {
    className: "euiRangeDraggle__inner",
    onMouseDown: handleMouseDown,
    onTouchStart: handleInteraction,
    onTouchMove: handleInteraction
  }));
};
EuiRangeDraggable.propTypes = {
  min: PropTypes.number.isRequired,
  max: PropTypes.number.isRequired,
  value: PropTypes.arrayOf(PropTypes.number.isRequired).isRequired,
  disabled: PropTypes.bool,
  compressed: PropTypes.bool,
  showTicks: PropTypes.bool,
  lowerPosition: PropTypes.string.isRequired,
  upperPosition: PropTypes.string.isRequired,
  onChange: PropTypes.func.isRequired
};