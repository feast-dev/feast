import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useCallback } from 'react';
import classNames from 'classnames';
import { EuiCallOut } from '../call_out';
import { EuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiForm = function EuiForm(_ref) {
  var children = _ref.children,
      className = _ref.className,
      isInvalid = _ref.isInvalid,
      error = _ref.error,
      _ref$component = _ref.component,
      component = _ref$component === void 0 ? 'div' : _ref$component,
      _ref$invalidCallout = _ref.invalidCallout,
      invalidCallout = _ref$invalidCallout === void 0 ? 'above' : _ref$invalidCallout,
      rest = _objectWithoutProperties(_ref, ["children", "className", "isInvalid", "error", "component", "invalidCallout"]);

  var handleFocus = useCallback(function (node) {
    node === null || node === void 0 ? void 0 : node.focus();
  }, []);
  var classes = classNames('euiForm', className);
  var optionalErrors = null;

  if (error) {
    var errorTexts = Array.isArray(error) ? error : [error];
    optionalErrors = ___EmotionJSX("ul", null, errorTexts.map(function (error, index) {
      return ___EmotionJSX("li", {
        className: "euiForm__error",
        key: index
      }, error);
    }));
  }

  var optionalErrorAlert;

  if (isInvalid && invalidCallout === 'above') {
    optionalErrorAlert = ___EmotionJSX(EuiI18n, {
      token: "euiForm.addressFormErrors",
      default: "Please address the highlighted errors."
    }, function (addressFormErrors) {
      return ___EmotionJSX(EuiCallOut, {
        tabIndex: -1,
        ref: handleFocus,
        className: "euiForm__errors",
        title: addressFormErrors,
        color: "danger",
        role: "alert",
        "aria-live": "assertive"
      }, optionalErrors);
    });
  }

  var Element = component;
  return ___EmotionJSX(Element, _extends({
    className: classes
  }, rest), optionalErrorAlert, children);
};