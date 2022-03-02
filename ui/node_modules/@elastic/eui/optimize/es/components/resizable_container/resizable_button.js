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
import React, { useCallback, useMemo, useRef } from 'react';
import classNames from 'classnames';
import { EuiI18n } from '../i18n';
import { useGeneratedHtmlId } from '../../services';
import { useEuiResizableContainerContext } from './context';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiResizableButton = function EuiResizableButton(_ref) {
  var isHorizontal = _ref.isHorizontal,
      className = _ref.className,
      id = _ref.id,
      registration = _ref.registration,
      disabled = _ref.disabled,
      onFocus = _ref.onFocus,
      onBlur = _ref.onBlur,
      rest = _objectWithoutProperties(_ref, ["isHorizontal", "className", "id", "registration", "disabled", "onFocus", "onBlur"]);

  var resizerId = useGeneratedHtmlId({
    prefix: 'resizable-button',
    conditionalId: id
  });

  var _useEuiResizableConta = useEuiResizableContainerContext(),
      _useEuiResizableConta2 = _useEuiResizableConta.registry;

  _useEuiResizableConta2 = _useEuiResizableConta2 === void 0 ? {
    resizers: {}
  } : _useEuiResizableConta2;
  var resizers = _useEuiResizableConta2.resizers;
  var isDisabled = useMemo(function () {
    return disabled || resizers[resizerId] && resizers[resizerId].isDisabled;
  }, [resizers, resizerId, disabled]);
  var classes = classNames('euiResizableButton', {
    'euiResizableButton--vertical': !isHorizontal,
    'euiResizableButton--horizontal': isHorizontal,
    'euiResizableButton--disabled': isDisabled
  }, className);
  var previousRef = useRef();
  var onRef = useCallback(function (ref) {
    if (!registration) return;

    if (ref) {
      previousRef.current = ref;
      registration.register({
        id: resizerId,
        ref: ref,
        isFocused: false,
        isDisabled: disabled || false
      });
    } else {
      if (previousRef.current != null) {
        registration.deregister(resizerId);
        previousRef.current = undefined;
      }
    }
  }, [registration, resizerId, disabled]);

  var setFocus = function setFocus(e) {
    return e.currentTarget.focus();
  };

  var handleFocus = function handleFocus() {
    onFocus && onFocus(resizerId);
  };

  return ___EmotionJSX(EuiI18n, {
    tokens: ['euiResizableButton.horizontalResizerAriaLabel', 'euiResizableButton.verticalResizerAriaLabel'],
    defaults: ['Press left or right to adjust panels size', 'Press up or down to adjust panels size']
  }, function (_ref2) {
    var _ref3 = _slicedToArray(_ref2, 2),
        horizontalResizerAriaLabel = _ref3[0],
        verticalResizerAriaLabel = _ref3[1];

    return ___EmotionJSX("button", _extends({
      id: resizerId,
      ref: onRef,
      "aria-label": isHorizontal ? horizontalResizerAriaLabel : verticalResizerAriaLabel,
      className: classes,
      "data-test-subj": "euiResizableButton",
      type: "button",
      onClick: setFocus,
      onFocus: handleFocus,
      onBlur: onBlur,
      disabled: isDisabled
    }, rest));
  });
};
export function euiResizableButtonWithControls(controls) {
  return function (props) {
    return ___EmotionJSX(EuiResizableButton, _extends({}, controls, props));
  };
}