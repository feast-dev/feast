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
import { EuiValidatableControl } from '../validatable_control';
import { jsx as ___EmotionJSX } from "@emotion/react";
var resizeToClassNameMap = {
  vertical: 'euiTextArea--resizeVertical',
  horizontal: 'euiTextArea--resizeHorizontal',
  both: 'euiTextArea--resizeBoth',
  none: 'euiTextArea--resizeNone'
};
export var RESIZE = Object.keys(resizeToClassNameMap);
export var EuiTextArea = function EuiTextArea(_ref) {
  var children = _ref.children,
      className = _ref.className,
      compressed = _ref.compressed,
      _ref$fullWidth = _ref.fullWidth,
      fullWidth = _ref$fullWidth === void 0 ? false : _ref$fullWidth,
      id = _ref.id,
      inputRef = _ref.inputRef,
      isInvalid = _ref.isInvalid,
      name = _ref.name,
      placeholder = _ref.placeholder,
      _ref$resize = _ref.resize,
      resize = _ref$resize === void 0 ? 'vertical' : _ref$resize,
      rows = _ref.rows,
      rest = _objectWithoutProperties(_ref, ["children", "className", "compressed", "fullWidth", "id", "inputRef", "isInvalid", "name", "placeholder", "resize", "rows"]);

  var classes = classNames('euiTextArea', resizeToClassNameMap[resize], {
    'euiTextArea--fullWidth': fullWidth,
    'euiTextArea--compressed': compressed
  }, className);
  var definedRows;

  if (rows) {
    definedRows = rows;
  } else if (compressed) {
    definedRows = 3;
  } else {
    definedRows = 6;
  }

  return ___EmotionJSX(EuiValidatableControl, {
    isInvalid: isInvalid
  }, ___EmotionJSX("textarea", _extends({
    className: classes
  }, rest, {
    rows: definedRows,
    name: name,
    id: id,
    ref: inputRef,
    placeholder: placeholder
  }), children));
};