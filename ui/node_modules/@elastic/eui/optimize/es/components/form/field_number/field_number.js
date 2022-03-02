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
import { EuiFormControlLayout } from '../form_control_layout';
import { EuiValidatableControl } from '../validatable_control';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiFieldNumber = function EuiFieldNumber(_ref) {
  var className = _ref.className,
      icon = _ref.icon,
      id = _ref.id,
      placeholder = _ref.placeholder,
      name = _ref.name,
      min = _ref.min,
      max = _ref.max,
      value = _ref.value,
      isInvalid = _ref.isInvalid,
      _ref$fullWidth = _ref.fullWidth,
      fullWidth = _ref$fullWidth === void 0 ? false : _ref$fullWidth,
      _ref$isLoading = _ref.isLoading,
      isLoading = _ref$isLoading === void 0 ? false : _ref$isLoading,
      _ref$compressed = _ref.compressed,
      compressed = _ref$compressed === void 0 ? false : _ref$compressed,
      prepend = _ref.prepend,
      append = _ref.append,
      inputRef = _ref.inputRef,
      readOnly = _ref.readOnly,
      controlOnly = _ref.controlOnly,
      rest = _objectWithoutProperties(_ref, ["className", "icon", "id", "placeholder", "name", "min", "max", "value", "isInvalid", "fullWidth", "isLoading", "compressed", "prepend", "append", "inputRef", "readOnly", "controlOnly"]);

  var classes = classNames('euiFieldNumber', className, {
    'euiFieldNumber--withIcon': icon,
    'euiFieldNumber--fullWidth': fullWidth,
    'euiFieldNumber--compressed': compressed,
    'euiFieldNumber--inGroup': prepend || append,
    'euiFieldNumber-isLoading': isLoading
  });

  var control = ___EmotionJSX(EuiValidatableControl, {
    isInvalid: isInvalid
  }, ___EmotionJSX("input", _extends({
    type: "number",
    id: id,
    min: min,
    max: max,
    name: name,
    value: value,
    placeholder: placeholder,
    readOnly: readOnly,
    className: classes,
    ref: inputRef
  }, rest)));

  if (controlOnly) {
    return control;
  }

  return ___EmotionJSX(EuiFormControlLayout, {
    icon: icon,
    fullWidth: fullWidth,
    isLoading: isLoading,
    compressed: compressed,
    readOnly: readOnly,
    prepend: prepend,
    append: append,
    inputId: id
  }, control);
};