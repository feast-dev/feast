import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import classNames from 'classnames';
import { EuiButtonEmpty } from '../button/button_empty';
import { EuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiCardSelect = function EuiCardSelect(_ref) {
  var className = _ref.className,
      _ref$isSelected = _ref.isSelected,
      isSelected = _ref$isSelected === void 0 ? false : _ref$isSelected,
      isDisabled = _ref.isDisabled,
      color = _ref.color,
      children = _ref.children,
      rest = _objectWithoutProperties(_ref, ["className", "isSelected", "isDisabled", "color", "children"]);

  var child = euiCardSelectableText(isSelected, isDisabled, children);
  var selectClasses = classNames('euiCardSelect', "euiCardSelect--".concat(euiCardSelectableColor(color, isSelected)), className);
  return ___EmotionJSX(EuiButtonEmpty, _extends({
    className: selectClasses,
    color: color || 'text',
    size: "xs",
    isDisabled: isDisabled,
    iconType: isSelected ? 'check' : undefined,
    role: "switch",
    "aria-checked": isSelected
  }, rest), child);
};

function euiCardSelectableText(isSelected, isDisabled, children) {
  if (children) {
    return children;
  }

  var text;

  if (isSelected) {
    text = ___EmotionJSX(EuiI18n, {
      token: "euiCardSelect.selected",
      default: "Selected"
    });
  } else if (isDisabled) {
    text = ___EmotionJSX(EuiI18n, {
      token: "euiCardSelect.unavailable",
      default: "Unavailable"
    });
  } else {
    text = ___EmotionJSX(EuiI18n, {
      token: "euiCardSelect.select",
      default: "Select"
    });
  }

  return text;
}

export function euiCardSelectableColor(color, isSelected) {
  var calculatedColor;

  if (color) {
    calculatedColor = color;
  } else if (isSelected) {
    calculatedColor = 'success';
  } else {
    calculatedColor = 'text';
  }

  return calculatedColor;
}