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
export var EuiFieldText = function EuiFieldText(_ref) {
  var id = _ref.id,
      name = _ref.name,
      placeholder = _ref.placeholder,
      value = _ref.value,
      className = _ref.className,
      icon = _ref.icon,
      isInvalid = _ref.isInvalid,
      inputRef = _ref.inputRef,
      _ref$fullWidth = _ref.fullWidth,
      fullWidth = _ref$fullWidth === void 0 ? false : _ref$fullWidth,
      isLoading = _ref.isLoading,
      compressed = _ref.compressed,
      prepend = _ref.prepend,
      append = _ref.append,
      readOnly = _ref.readOnly,
      controlOnly = _ref.controlOnly,
      rest = _objectWithoutProperties(_ref, ["id", "name", "placeholder", "value", "className", "icon", "isInvalid", "inputRef", "fullWidth", "isLoading", "compressed", "prepend", "append", "readOnly", "controlOnly"]);

  var classes = classNames('euiFieldText', className, {
    'euiFieldText--withIcon': icon,
    'euiFieldText--fullWidth': fullWidth,
    'euiFieldText--compressed': compressed,
    'euiFieldText--inGroup': prepend || append,
    'euiFieldText-isLoading': isLoading
  });

  var control = ___EmotionJSX(EuiValidatableControl, {
    isInvalid: isInvalid
  }, ___EmotionJSX("input", _extends({
    type: "text",
    id: id,
    name: name,
    placeholder: placeholder,
    className: classes,
    value: value,
    ref: inputRef,
    readOnly: readOnly
  }, rest)));

  if (controlOnly) return control;
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