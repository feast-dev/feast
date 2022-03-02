import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";
import _extends from "@babel/runtime/helpers/extends";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useState } from 'react';
import classNames from 'classnames';
import { EuiFormControlLayout } from '../form_control_layout';
import { EuiValidatableControl } from '../validatable_control';
import { EuiButtonIcon } from '../../button';
import { useEuiI18n } from '../../i18n';
import { useCombinedRefs } from '../../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiFieldPassword = function EuiFieldPassword(_ref) {
  var className = _ref.className,
      id = _ref.id,
      name = _ref.name,
      placeholder = _ref.placeholder,
      value = _ref.value,
      isInvalid = _ref.isInvalid,
      fullWidth = _ref.fullWidth,
      isLoading = _ref.isLoading,
      compressed = _ref.compressed,
      _inputRef = _ref.inputRef,
      prepend = _ref.prepend,
      append = _ref.append,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'password' : _ref$type,
      dualToggleProps = _ref.dualToggleProps,
      rest = _objectWithoutProperties(_ref, ["className", "id", "name", "placeholder", "value", "isInvalid", "fullWidth", "isLoading", "compressed", "inputRef", "prepend", "append", "type", "dualToggleProps"]);

  // Set the initial input type to `password` if they want dual
  var _useState = useState(type === 'dual' ? 'password' : type),
      _useState2 = _slicedToArray(_useState, 2),
      inputType = _useState2[0],
      setInputType = _useState2[1]; // Setup toggle aria-label


  var _useEuiI18n = useEuiI18n(['euiFieldPassword.showPassword', 'euiFieldPassword.maskPassword'], ['Show password as plain text. Note: this will visually expose your password on the screen.', 'Mask password']),
      _useEuiI18n2 = _slicedToArray(_useEuiI18n, 2),
      showPasswordLabel = _useEuiI18n2[0],
      maskPasswordLabel = _useEuiI18n2[1]; // Setup the inputRef to auto-focus when toggling visibility


  var _useState3 = useState(null),
      _useState4 = _slicedToArray(_useState3, 2),
      inputRef = _useState4[0],
      _setInputRef = _useState4[1];

  var setInputRef = useCombinedRefs([_setInputRef, _inputRef]);

  var handleToggle = function handleToggle(event, isVisible) {
    setInputType(isVisible ? 'password' : 'text');

    if (inputRef) {
      inputRef.focus();
    }

    if (dualToggleProps && dualToggleProps.onClick) {
      dualToggleProps.onClick(event);
    }
  }; // Convert any `append` elements to an array so the visibility
  // toggle can be added to it


  var appends = Array.isArray(append) ? append : [];
  if (append && !Array.isArray(append)) appends.push(append); // Add a toggling button to switch between `password` and `input` if consumer wants `dual`
  // https://www.w3schools.com/howto/howto_js_toggle_password.asp

  if (type === 'dual') {
    var isVisible = inputType === 'text';

    var visibilityToggle = ___EmotionJSX(EuiButtonIcon, _extends({
      iconType: isVisible ? 'eyeClosed' : 'eye',
      "aria-label": isVisible ? maskPasswordLabel : showPasswordLabel,
      title: isVisible ? maskPasswordLabel : showPasswordLabel,
      disabled: rest.disabled
    }, dualToggleProps, {
      onClick: function onClick(e) {
        return handleToggle(e, isVisible);
      }
    }));

    appends = [].concat(_toConsumableArray(appends), [visibilityToggle]);
  }

  var finalAppend = appends.length ? appends : undefined;
  var classes = classNames('euiFieldPassword', {
    'euiFieldPassword--fullWidth': fullWidth,
    'euiFieldPassword--compressed': compressed,
    'euiFieldPassword-isLoading': isLoading,
    'euiFieldPassword--inGroup': prepend || finalAppend,
    'euiFieldPassword--withToggle': type === 'dual'
  }, className);
  return ___EmotionJSX(EuiFormControlLayout, {
    icon: "lock",
    fullWidth: fullWidth,
    isLoading: isLoading,
    compressed: compressed,
    prepend: prepend,
    append: finalAppend
  }, ___EmotionJSX(EuiValidatableControl, {
    isInvalid: isInvalid
  }, ___EmotionJSX("input", _extends({
    type: inputType,
    id: id,
    name: name,
    placeholder: placeholder,
    className: classes,
    value: value,
    ref: setInputRef
  }, rest))));
};
EuiFieldPassword.defaultProps = {
  value: undefined,
  fullWidth: false,
  isLoading: false,
  compressed: false
};