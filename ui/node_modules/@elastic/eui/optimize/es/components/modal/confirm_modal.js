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
import React, { useEffect, useState } from 'react';
import classnames from 'classnames';
import { EuiModal } from './modal';
import { EuiModalFooter } from './modal_footer';
import { EuiModalHeader } from './modal_header';
import { EuiModalHeaderTitle } from './modal_header_title';
import { EuiModalBody } from './modal_body';
import { EuiButton, EuiButtonEmpty } from '../button';
import { EuiText } from '../text';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var CONFIRM_BUTTON = 'confirm';
export var CANCEL_BUTTON = 'cancel';
export var EuiConfirmModal = function EuiConfirmModal(_ref) {
  var children = _ref.children,
      title = _ref.title,
      onCancel = _ref.onCancel,
      onConfirm = _ref.onConfirm,
      cancelButtonText = _ref.cancelButtonText,
      confirmButtonText = _ref.confirmButtonText,
      confirmButtonDisabled = _ref.confirmButtonDisabled,
      className = _ref.className,
      _ref$buttonColor = _ref.buttonColor,
      buttonColor = _ref$buttonColor === void 0 ? 'primary' : _ref$buttonColor,
      defaultFocusedButton = _ref.defaultFocusedButton,
      isLoading = _ref.isLoading,
      rest = _objectWithoutProperties(_ref, ["children", "title", "onCancel", "onConfirm", "cancelButtonText", "confirmButtonText", "confirmButtonDisabled", "className", "buttonColor", "defaultFocusedButton", "isLoading"]);

  var _useState = useState(null),
      _useState2 = _slicedToArray(_useState, 2),
      cancelButton = _useState2[0],
      setCancelButton = _useState2[1];

  var _useState3 = useState(null),
      _useState4 = _slicedToArray(_useState3, 2),
      confirmButton = _useState4[0],
      setConfirmButton = _useState4[1];

  useEffect(function () {
    // We have to do this instead of using `autoFocus` because React's polyfill for auto-focusing
    // elements conflicts with the focus-trap logic we have on EuiModal.
    // Wait a beat for the focus-trap to complete, and then set focus to the right button. Check that
    // the buttons exist first, because it's possible the modal has been closed already.
    requestAnimationFrame(function () {
      if (defaultFocusedButton === CANCEL_BUTTON && cancelButton) {
        cancelButton.focus();
      } else if (defaultFocusedButton === CONFIRM_BUTTON && confirmButton) {
        confirmButton.focus();
      }
    });
  });

  var confirmRef = function confirmRef(node) {
    return setConfirmButton(node);
  };

  var cancelRef = function cancelRef(node) {
    return setCancelButton(node);
  };

  var classes = classnames('euiModal--confirmation', className);
  var modalTitle;

  if (title) {
    modalTitle = ___EmotionJSX(EuiModalHeader, null, ___EmotionJSX(EuiModalHeaderTitle, {
      "data-test-subj": "confirmModalTitleText"
    }, title));
  }

  var message;

  if (typeof children === 'string' && children.length > 0) {
    message = ___EmotionJSX("p", null, children);
  } else {
    message = children;
  }

  return ___EmotionJSX(EuiModal, _extends({
    className: classes,
    onClose: onCancel
  }, rest), modalTitle, message && ___EmotionJSX(EuiModalBody, null, ___EmotionJSX(EuiText, {
    "data-test-subj": "confirmModalBodyText"
  }, message)), ___EmotionJSX(EuiModalFooter, null, ___EmotionJSX(EuiButtonEmpty, {
    "data-test-subj": "confirmModalCancelButton",
    onClick: onCancel,
    buttonRef: cancelRef
  }, cancelButtonText), ___EmotionJSX(EuiButton, {
    "data-test-subj": "confirmModalConfirmButton",
    onClick: onConfirm,
    isLoading: isLoading,
    fill: true,
    buttonRef: confirmRef,
    color: buttonColor,
    isDisabled: confirmButtonDisabled
  }, confirmButtonText)));
};