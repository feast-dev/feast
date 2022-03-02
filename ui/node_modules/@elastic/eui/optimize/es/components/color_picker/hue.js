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