import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { forwardRef, useEffect, useRef, useState } from 'react';
import classNames from 'classnames';
import { keys, useMouseMove } from '../../services';
import { isNil } from '../../services/predicate';
import { useEuiI18n } from '../i18n';
import { getEventPosition } from './utils';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiSaturation = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var className = _ref.className,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? [1, 0, 0] : _ref$color,
      _ref$dataTestSubj = _ref['data-test-subj'],
      dataTestSubj = _ref$dataTestSubj === void 0 ? 'euiSaturation' : _ref$dataTestSubj,
      hex = _ref.hex,
      id = _ref.id,
      onChange = _ref.onChange,
      rest = _objectWithoutProperties(_ref, ["className", "color", "data-test-subj", "hex", "id", "onChange"]);

  var _useEuiI18n = useEuiI18n(['euiSaturation.ariaLabel', 'euiSaturation.screenReaderInstructions'], ['HSV color mode saturation and value 2-axis slider', "Arrow keys to navigate the square color gradient. Coordinates will be used to calculate HSV color mode 'saturation' and 'value' numbers, in the range of 0 to 1. Left and right to change the saturation. Up and down change the value."]),
      _useEuiI18n2 = _slicedToArray(_useEuiI18n, 2),
      roleDescString = _useEuiI18n2[0],
      instructionsString = _useEuiI18n2[1];

  var _useState = useState({
    left: 0,
    top: 0
  }),
      _useState2 = _slicedToArray(_useState, 2),
      indicator = _useState2[0],
      setIndicator = _useState2[1];

  var _useState3 = useState([]),
      _useState4 = _slicedToArray(_useState3, 2),
      lastColor = _useState4[0],
      setLastColor = _useState4[1];

  var boxRef = useRef(null);
  useEffect(function () {
    // Mimics `componentDidMount` and `componentDidUpdate`
    var _color = _slicedToArray(color, 3),
        s = _color[1],
        v = _color[2];

    if (!isNil(boxRef.current) && lastColor.join() !== color.join()) {
      var _boxRef$current$getBo = boxRef.current.getBoundingClientRect(),
          height = _boxRef$current$getBo.height,
          width = _boxRef$current$getBo.width;

      setIndicator({
        left: s * width,
        top: (1 - v) * height
      });
    }
  }, [color, lastColor]);

  var calculateColor = function calculateColor(_ref2) {
    var top = _ref2.top,
        height = _ref2.height,
        left = _ref2.left,
        width = _ref2.width;

    var _color2 = _slicedToArray(color, 1),
        h = _color2[0];

    var s = left / width;
    var v = 1 - top / height;
    return [h, s, v];
  };

  var handleUpdate = function handleUpdate(box) {
    var left = box.left,
        top = box.top;
    setIndicator({
      left: left,
      top: top
    });
    var newColor = calculateColor(box);
    setLastColor(newColor);
    onChange(newColor);
  };

  var handleChange = function handleChange(location) {
    if (isNil(boxRef === null || boxRef === void 0 ? void 0 : boxRef.current)) return;
    var box = getEventPosition(location, boxRef.current);
    handleUpdate(box);
  };

  var _useMouseMove = useMouseMove(handleChange, boxRef.current),
      _useMouseMove2 = _slicedToArray(_useMouseMove, 2),
      handleMouseDown = _useMouseMove2[0],
      handleInteraction = _useMouseMove2[1];

  var handleKeyDown = function handleKeyDown(event) {
    if (isNil(boxRef === null || boxRef === void 0 ? void 0 : boxRef.current)) return;

    var _boxRef$current$getBo2 = boxRef.current.getBoundingClientRect(),
        height = _boxRef$current$getBo2.height,
        width = _boxRef$current$getBo2.width;

    var left = indicator.left,
        top = indicator.top;
    var heightScale = height / 100;
    var widthScale = width / 100;
    var newLeft = left;
    var newTop = top;

    switch (event.key) {
      case keys.ARROW_DOWN:
        event.preventDefault();
        newTop = top < height ? top + heightScale : height;
        break;

      case keys.ARROW_LEFT:
        event.preventDefault();
        newLeft = left > 0 ? left - widthScale : 0;
        break;

      case keys.ARROW_UP:
        event.preventDefault();
        newTop = top > 0 ? top - heightScale : 0;
        break;

      case keys.ARROW_RIGHT:
        event.preventDefault();
        newLeft = left < width ? left + widthScale : width;
        break;

      default:
        return;
    }

    var newPosition = {
      left: newLeft,
      top: newTop
    };
    setIndicator(newPosition);
    var newColor = calculateColor(_objectSpread({
      width: width,
      height: height
    }, newPosition));
    onChange(newColor);
  };

  var classes = classNames('euiSaturation', className);
  var instructionsId = "".concat(id, "-instructions");
  return ___EmotionJSX("div", _extends({
    onMouseDown: handleMouseDown,
    onTouchStart: handleInteraction,
    onTouchMove: handleInteraction,
    onKeyDown: handleKeyDown,
    ref: ref,
    className: classes,
    "data-test-subj": dataTestSubj,
    style: {
      background: "hsl(".concat(color[0], ", 100%, 50%)")
    },
    tabIndex: -1
  }, rest), ___EmotionJSX("div", {
    className: "euiSaturation__lightness",
    ref: boxRef
  }, ___EmotionJSX("div", {
    className: "euiSaturation__saturation"
  })), ___EmotionJSX("button", {
    id: "".concat(id, "-saturationIndicator"),
    className: "euiSaturation__indicator",
    style: _objectSpread({}, indicator),
    "aria-roledescription": roleDescString,
    "aria-label": hex,
    "aria-describedby": instructionsId
  }), ___EmotionJSX("span", {
    hidden: true,
    "aria-live": "assertive"
  }, hex), ___EmotionJSX("span", {
    hidden: true,
    id: instructionsId
  }, instructionsString));
});
EuiSaturation.displayName = 'EuiSaturation';