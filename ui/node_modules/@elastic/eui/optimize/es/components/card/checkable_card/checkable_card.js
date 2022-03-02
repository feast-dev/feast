import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useRef } from 'react';
import classNames from 'classnames';
import { EuiRadio, EuiCheckbox } from '../../form';
import { EuiSplitPanel } from '../../panel';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiCheckableCard = function EuiCheckableCard(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$checkableType = _ref.checkableType,
      checkableType = _ref$checkableType === void 0 ? 'radio' : _ref$checkableType,
      label = _ref.label,
      checked = _ref.checked,
      disabled = _ref.disabled,
      hasShadow = _ref.hasShadow,
      _ref$hasBorder = _ref.hasBorder,
      hasBorder = _ref$hasBorder === void 0 ? true : _ref$hasBorder,
      rest = _objectWithoutProperties(_ref, ["children", "className", "checkableType", "label", "checked", "disabled", "hasShadow", "hasBorder"]);

  var id = rest.id;
  var labelEl = useRef(null);
  var classes = classNames('euiCheckableCard', {
    'euiCheckableCard-isChecked': checked,
    'euiCheckableCard-isDisabled': disabled
  }, className);
  var checkableElement;

  if (checkableType === 'radio') {
    checkableElement = ___EmotionJSX(EuiRadio, _extends({
      checked: checked,
      disabled: disabled
    }, rest));
  } else {
    checkableElement = ___EmotionJSX(EuiCheckbox, _extends({
      checked: checked,
      disabled: disabled
    }, rest));
  }

  var labelClasses = classNames('euiCheckableCard__label', {
    'euiCheckableCard__label-isDisabled': disabled
  });

  var onChangeAffordance = function onChangeAffordance() {
    if (labelEl.current) {
      labelEl.current.click();
    }
  };

  return ___EmotionJSX(EuiSplitPanel.Outer, {
    responsive: false,
    hasShadow: hasShadow,
    hasBorder: hasBorder,
    direction: "row",
    className: classes
  }, ___EmotionJSX(EuiSplitPanel.Inner, {
    // Bubbles up the change event when clicking on the whole div for extra affordance
    onClick: disabled ? undefined : onChangeAffordance,
    color: checked ? 'primary' : 'subdued',
    grow: false
  }, checkableElement), ___EmotionJSX(EuiSplitPanel.Inner, null, ___EmotionJSX("label", {
    ref: labelEl,
    className: labelClasses,
    htmlFor: id,
    "aria-describedby": children ? "".concat(id, "-details") : undefined
  }, label), children && ___EmotionJSX("div", {
    id: "".concat(id, "-details"),
    className: "euiCheckableCard__children"
  }, children)));
};