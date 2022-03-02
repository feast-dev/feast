function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

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
EuiTextArea.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  isInvalid: PropTypes.bool,
  fullWidth: PropTypes.bool,
  compressed: PropTypes.bool,

  /**
       * Which direction, if at all, should the textarea resize
       */
  resize: PropTypes.oneOf(["vertical", "horizontal", "both", "none"]),
  inputRef: PropTypes.any
};