import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useState } from 'react';
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