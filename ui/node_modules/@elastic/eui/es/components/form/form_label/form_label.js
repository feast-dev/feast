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
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiFormLabel = function EuiFormLabel(_ref) {
  var _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'label' : _ref$type,
      isFocused = _ref.isFocused,
      isInvalid = _ref.isInvalid,
      isDisabled = _ref.isDisabled,
      children = _ref.children,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["type", "isFocused", "isInvalid", "isDisabled", "children", "className"]);

  var classes = classNames('euiFormLabel', className, {
    'euiFormLabel-isFocused': isFocused,
    'euiFormLabel-isInvalid': isInvalid,
    'euiFormLabel-isDisabled': isDisabled
  });

  if (type === 'legend') {
    return ___EmotionJSX("legend", _extends({
      className: classes
    }, rest), children);
  } else {
    return ___EmotionJSX("label", _extends({
      className: classes
    }, rest), children);
  }
};
EuiFormLabel.propTypes = {
  /**
     * Default type is a `label` but can be changed to a `legend`
     * if using inside a `fieldset`.
     */

  /**
     * Default type is a `label` but can be changed to a `legend`
     * if using inside a `fieldset`.
     */
  type: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.oneOf(["label"]), PropTypes.oneOf(["label", "legend"])]), PropTypes.oneOfType([PropTypes.oneOf(["legend"]).isRequired, PropTypes.oneOf(["label", "legend"])])]),
  isFocused: PropTypes.bool,
  isInvalid: PropTypes.bool,

  /**
     * Changes `cursor` to `default`.
     */

  /**
     * Changes `cursor` to `default`.
     */
  isDisabled: PropTypes.bool,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};