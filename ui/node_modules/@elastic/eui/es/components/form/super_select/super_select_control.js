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
import React, { Fragment } from 'react';
import classNames from 'classnames';
import { EuiScreenReaderOnly } from '../../accessibility';
import { EuiFormControlLayout } from '../form_control_layout';
import { EuiI18n } from '../../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiSuperSelectControl = function EuiSuperSelectControl(_ref) {
  var className = _ref.className,
      _ref$options = _ref.options,
      options = _ref$options === void 0 ? [] : _ref$options,
      id = _ref.id,
      name = _ref.name,
      _ref$fullWidth = _ref.fullWidth,
      fullWidth = _ref$fullWidth === void 0 ? false : _ref$fullWidth,
      _ref$isLoading = _ref.isLoading,
      isLoading = _ref$isLoading === void 0 ? false : _ref$isLoading,
      _ref$isInvalid = _ref.isInvalid,
      isInvalid = _ref$isInvalid === void 0 ? false : _ref$isInvalid,
      defaultValue = _ref.defaultValue,
      _ref$compressed = _ref.compressed,
      compressed = _ref$compressed === void 0 ? false : _ref$compressed,
      value = _ref.value,
      prepend = _ref.prepend,
      append = _ref.append,
      screenReaderId = _ref.screenReaderId,
      rest = _objectWithoutProperties(_ref, ["className", "options", "id", "name", "fullWidth", "isLoading", "isInvalid", "defaultValue", "compressed", "value", "prepend", "append", "screenReaderId"]);

  var classes = classNames('euiSuperSelectControl', {
    'euiSuperSelectControl--fullWidth': fullWidth,
    'euiSuperSelectControl--compressed': compressed,
    'euiSuperSelectControl--inGroup': prepend || append,
    'euiSuperSelectControl-isLoading': isLoading,
    'euiSuperSelectControl-isInvalid': isInvalid
  }, className); // React HTML input can not have both value and defaultValue properties.
  // https://reactjs.org/docs/uncontrolled-components.html#default-values

  var selectDefaultValue;

  if (value == null) {
    selectDefaultValue = defaultValue || '';
  }

  var selectedValue;

  if (value) {
    var selectedOption = options.find(function (option) {
      return option.value === value;
    });
    selectedValue = selectedOption ? selectedOption.inputDisplay : selectedValue;
  }

  var icon = {
    type: 'arrowDown',
    side: 'right'
  };
  return ___EmotionJSX(Fragment, null, ___EmotionJSX("input", {
    type: "hidden",
    id: id,
    name: name,
    defaultValue: selectDefaultValue,
    value: value
  }), ___EmotionJSX(EuiFormControlLayout, {
    icon: icon,
    fullWidth: fullWidth,
    isLoading: isLoading,
    compressed: compressed,
    prepend: prepend,
    append: append
  }, ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", {
    id: screenReaderId
  }, ___EmotionJSX(EuiI18n, {
    token: "euiSuperSelectControl.selectAnOption",
    default: "Select an option: {selectedValue}, is selected",
    values: {
      selectedValue: selectedValue
    }
  }))), ___EmotionJSX("button", _extends({
    type: "button",
    className: classes,
    "aria-haspopup": "listbox"
  }, rest), selectedValue)));
};