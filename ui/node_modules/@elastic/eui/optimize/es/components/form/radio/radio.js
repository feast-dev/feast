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
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiRadio = function EuiRadio(_ref) {
  var className = _ref.className,
      id = _ref.id,
      name = _ref.name,
      checked = _ref.checked,
      label = _ref.label,
      value = _ref.value,
      onChange = _ref.onChange,
      disabled = _ref.disabled,
      compressed = _ref.compressed,
      autoFocus = _ref.autoFocus,
      labelProps = _ref.labelProps,
      rest = _objectWithoutProperties(_ref, ["className", "id", "name", "checked", "label", "value", "onChange", "disabled", "compressed", "autoFocus", "labelProps"]);

  var classes = classNames('euiRadio', {
    'euiRadio--noLabel': !label,
    'euiRadio--compressed': compressed
  }, className);
  var labelClasses = classNames('euiRadio__label', labelProps === null || labelProps === void 0 ? void 0 : labelProps.className);
  var optionalLabel;

  if (label) {
    optionalLabel = ___EmotionJSX("label", _extends({}, labelProps, {
      className: labelClasses,
      htmlFor: id
    }), label);
  }

  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), ___EmotionJSX("input", {
    className: "euiRadio__input",
    type: "radio",
    id: id,
    name: name,
    value: value,
    checked: checked,
    onChange: onChange,
    disabled: disabled,
    autoFocus: autoFocus
  }), ___EmotionJSX("div", {
    className: "euiRadio__circle"
  }), optionalLabel);
};