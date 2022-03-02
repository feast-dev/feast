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
import { keysOf } from '../../common';
import { EuiIcon } from '../../icon';
import { EuiI18n } from '../../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  s: 'euiFormControlLayoutClearButton--small',
  m: null
};
export var SIZES = keysOf(sizeToClassNameMap);
export var EuiFormControlLayoutClearButton = function EuiFormControlLayoutClearButton(_ref) {
  var className = _ref.className,
      onClick = _ref.onClick,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      rest = _objectWithoutProperties(_ref, ["className", "onClick", "size"]);

  var classes = classNames('euiFormControlLayoutClearButton', sizeToClassNameMap[size], className);
  return ___EmotionJSX(EuiI18n, {
    token: "euiFormControlLayoutClearButton.label",
    default: "Clear input"
  }, function (label) {
    return ___EmotionJSX("button", _extends({
      type: "button",
      className: classes,
      onClick: onClick,
      "aria-label": label
    }, rest), ___EmotionJSX(EuiIcon, {
      className: "euiFormControlLayoutClearButton__icon",
      type: "cross"
    }));
  });
};
EuiFormControlLayoutClearButton.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  size: PropTypes.any
};