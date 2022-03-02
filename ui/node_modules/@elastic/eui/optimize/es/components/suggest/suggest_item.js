import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { keysOf } from '../common';
import classNames from 'classnames';
import { EuiIcon } from '../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
var colorToClassNameMap = {
  tint0: 'euiSuggestItem__type--tint0',
  tint1: 'euiSuggestItem__type--tint1',
  tint2: 'euiSuggestItem__type--tint2',
  tint3: 'euiSuggestItem__type--tint3',
  tint4: 'euiSuggestItem__type--tint4',
  tint5: 'euiSuggestItem__type--tint5',
  tint6: 'euiSuggestItem__type--tint6',
  tint7: 'euiSuggestItem__type--tint7',
  tint8: 'euiSuggestItem__type--tint8',
  tint9: 'euiSuggestItem__type--tint9',
  tint10: 'euiSuggestItem__type--tint10'
};
export var COLORS = keysOf(colorToClassNameMap);
var labelDisplayToClassMap = {
  fixed: 'euiSuggestItem__labelDisplay--fixed',
  expand: 'euiSuggestItem__labelDisplay--expand'
};
var descriptionDisplayToClassMap = {
  truncate: 'euiSuggestItem__description--truncate',
  wrap: 'euiSuggestItem__description--wrap'
};
export var DISPLAYS = keysOf(labelDisplayToClassMap);
export var EuiSuggestItem = function EuiSuggestItem(_ref) {
  var className = _ref.className,
      label = _ref.label,
      type = _ref.type,
      _ref$labelDisplay = _ref.labelDisplay,
      labelDisplay = _ref$labelDisplay === void 0 ? 'fixed' : _ref$labelDisplay,
      _ref$labelWidth = _ref.labelWidth,
      labelWidth = _ref$labelWidth === void 0 ? '50' : _ref$labelWidth,
      description = _ref.description,
      _ref$descriptionDispl = _ref.descriptionDisplay,
      descriptionDisplay = _ref$descriptionDispl === void 0 ? 'truncate' : _ref$descriptionDispl,
      onClick = _ref.onClick,
      rest = _objectWithoutProperties(_ref, ["className", "label", "type", "labelDisplay", "labelWidth", "description", "descriptionDisplay", "onClick"]);

  var classes = classNames('euiSuggestItem', {
    'euiSuggestItem-isClickable': onClick
  }, className);
  var colorClass = '';
  var labelDisplayCalculated = !description ? 'expand' : labelDisplay;
  var labelClassNames = classNames('euiSuggestItem__label', labelDisplayToClassMap[labelDisplayCalculated], _defineProperty({}, "euiSuggestItem__label--width".concat(labelWidth), labelDisplay === 'fixed'));
  var descriptionClassNames = classNames('euiSuggestItem__description', descriptionDisplayToClassMap[descriptionDisplay]);

  if (type && type.color) {
    if (COLORS.indexOf(type.color) > -1) {
      colorClass = colorToClassNameMap[type.color];
    }
  }

  var innerContent = ___EmotionJSX(React.Fragment, null, ___EmotionJSX("span", {
    className: "euiSuggestItem__type ".concat(colorClass)
  }, ___EmotionJSX(EuiIcon, {
    type: type.iconType,
    color: "inherit" // forces the icon to inherit its parent color

  })), ___EmotionJSX("span", {
    className: labelClassNames
  }, label), description && ___EmotionJSX("span", {
    className: descriptionClassNames
  }, description));

  if (onClick) {
    return ___EmotionJSX("button", _extends({
      onClick: onClick,
      className: classes
    }, rest), innerContent);
  } else {
    return ___EmotionJSX("div", _extends({
      className: classes
    }, rest), innerContent);
  }
};