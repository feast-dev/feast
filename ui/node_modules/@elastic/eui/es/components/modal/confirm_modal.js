function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useEffect, useState } from 'react';
import PropTypes from "prop-types";
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
EuiConfirmModal.propTypes = {
  /**
     * ReactNode to render as this component's content
     */
  children: PropTypes.node,
  title: PropTypes.node,
  cancelButtonText: PropTypes.node,
  confirmButtonText: PropTypes.node,
  onCancel: PropTypes.func.isRequired,
  onConfirm: PropTypes.func,
  confirmButtonDisabled: PropTypes.bool,
  className: PropTypes.string,
  defaultFocusedButton: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.any.isRequired]),
  buttonColor: PropTypes.oneOf(["primary", "accent", "success", "warning", "danger", "ghost", "text"]),
  // For docs only, will get passed with ...rest

  /**
     * Sets the max-width of the modal.
     * Set to `true` to use the default (`euiBreakpoints 'm'`),
     * set to `false` to not restrict the width,
     * set to a number for a custom width in px,
     * set to a string for a custom width in custom measurement.
     */
  maxWidth: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.number.isRequired, PropTypes.string.isRequired]),

  /**
     * Passes `isLoading` prop to the confirm button
     */
  isLoading: PropTypes.bool
};