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
export var EuiSelect = function EuiSelect(_ref) {
  var className = _ref.className,
      _ref$options = _ref.options,
      options = _ref$options === void 0 ? [] : _ref$options,
      id = _ref.id,
      name = _ref.name,
      inputRef = _ref.inputRef,
      isInvalid = _ref.isInvalid,
      _ref$fullWidth = _ref.fullWidth,
      fullWidth = _ref$fullWidth === void 0 ? false : _ref$fullWidth,
      _ref$isLoading = _ref.isLoading,
      isLoading = _ref$isLoading === void 0 ? false : _ref$isLoading,
      _ref$hasNoInitialSele = _ref.hasNoInitialSelection,
      hasNoInitialSelection = _ref$hasNoInitialSele === void 0 ? false : _ref$hasNoInitialSele,
      defaultValue = _ref.defaultValue,
      _ref$compressed = _ref.compressed,
      compressed = _ref$compressed === void 0 ? false : _ref$compressed,
      _value = _ref.value,
      prepend = _ref.prepend,
      append = _ref.append,
      onMouseUp = _ref.onMouseUp,
      rest = _objectWithoutProperties(_ref, ["className", "options", "id", "name", "inputRef", "isInvalid", "fullWidth", "isLoading", "hasNoInitialSelection", "defaultValue", "compressed", "value", "prepend", "append", "onMouseUp"]);

  // if this is injecting an empty option for `hasNoInitialSelection` then
  // value needs to fallback to an empty string to interact properly with `defaultValue`
  var value = hasNoInitialSelection ? _value !== null && _value !== void 0 ? _value : '' : _value;

  var handleMouseUp = function handleMouseUp(e) {
    // Normalizes cross-browser mouse eventing by preventing propagation,
    // notably for use in conjunction with EuiOutsideClickDetector.
    // See https://github.com/elastic/eui/pull/1926 for full discussion on
    // rationale and alternatives should this intervention become problematic.
    e.nativeEvent.stopImmediatePropagation();
    if (onMouseUp) onMouseUp(e);
  };

  var classes = classNames('euiSelect', {
    'euiSelect--fullWidth': fullWidth,
    'euiSelect--compressed': compressed,
    'euiSelect--inGroup': prepend || append,
    'euiSelect-isLoading': isLoading
  }, className);
  var emptyOptionNode;

  if (hasNoInitialSelection) {
    emptyOptionNode = ___EmotionJSX("option", {
      value: "",
      disabled: true,
      hidden: true,
      style: {
        display: 'none'
      }
    }, "\xA0");
  } // React HTML input can not have both value and defaultValue properties.
  // https://reactjs.org/docs/uncontrolled-components.html#default-values


  var selectDefaultValue;

  if (value == null) {
    selectDefaultValue = defaultValue || '';
  }

  var icon = {
    type: 'arrowDown',
    side: 'right'
  };
  return ___EmotionJSX(EuiFormControlLayout, {
    icon: icon,
    fullWidth: fullWidth,
    isLoading: isLoading,
    compressed: compressed,
    prepend: prepend,
    append: append,
    inputId: id
  }, ___EmotionJSX(EuiValidatableControl, {
    isInvalid: isInvalid
  }, ___EmotionJSX("select", _extends({
    id: id,
    name: name,
    className: classes,
    ref: inputRef,
    defaultValue: selectDefaultValue,
    value: value,
    onMouseUp: handleMouseUp
  }, rest), emptyOptionNode, options.map(function (option, index) {
    var text = option.text,
        rest = _objectWithoutProperties(option, ["text"]);

    return ___EmotionJSX("option", _extends({}, rest, {
      key: index
    }), text);
  }))));
};