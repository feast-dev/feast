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
import { EuiScreenReaderOnly } from '../accessibility';
import { EuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
var HUE_RANGE = 359;
export var EuiHue = function EuiHue(_ref) {
  var className = _ref.className,
      hex = _ref.hex,
      _ref$hue = _ref.hue,
      hue = _ref$hue === void 0 ? 1 : _ref$hue,
      id = _ref.id,
      onChange = _ref.onChange,
      rest = _objectWithoutProperties(_ref, ["className", "hex", "hue", "id", "onChange"]);

  var handleChange = function handleChange(e) {
    onChange(Number(e.target.value));
  };

  var classes = classNames('euiHue', className);
  return ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("label", {
    htmlFor: "".concat(id, "-hue")
  }, ___EmotionJSX(EuiI18n, {
    token: "euiHue.label",
    default: "Select the HSV color mode 'hue' value"
  }))), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", {
    "aria-live": "polite"
  }, hex)), ___EmotionJSX("div", {
    className: classes
  }, ___EmotionJSX("input", _extends({
    id: "".concat(id, "-hue"),
    min: 0,
    max: HUE_RANGE,
    step: 1,
    type: "range",
    className: "euiHue__range",
    value: hue,
    onChange: handleChange
  }, rest))));
};
EuiHue.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  hex: PropTypes.string,
  hue: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired]),
  onChange: PropTypes.func.isRequired
};