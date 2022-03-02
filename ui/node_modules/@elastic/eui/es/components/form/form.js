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
import React, { useCallback } from 'react';
import PropTypes from "prop-types";
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
EuiForm.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Which HTML element to render `div` or `form`
       */
  component: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.oneOf(["form"]).isRequired, PropTypes.oneOf(["div"])]), PropTypes.oneOf(["form", "div"])]),
  isInvalid: PropTypes.bool,
  error: PropTypes.oneOfType([PropTypes.node.isRequired, PropTypes.arrayOf(PropTypes.node.isRequired).isRequired]),

  /**
       * Where to display the callout with the list of errors
       */
  invalidCallout: PropTypes.oneOf(["above", "none"])
};