import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// THIS IS A GENERATED FILE. DO NOT MODIFY MANUALLY. @see scripts/compile-icons.js
import * as React from 'react';
import { jsx as ___EmotionJSX } from "@emotion/react";

var EuiIconCloudStormy = function EuiIconCloudStormy(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    d: "M7.421 4.93a.5.5 0 11-.87.49 3 3 0 10-4.43 3.918.5.5 0 01-.626.78 4 4 0 013.973-6.84l.032.018V3.28a5.5 5.5 0 117.003 7.357.5.5 0 11-.36-.934 4.5 4.5 0 10-5.77-5.923c.42.31.778.701 1.05 1.15h-.002zM9.6 11c.669.002.794.67.36 1.003l-4.68 3.882c-.457.378-1.053-.26-.643-.689l3.08-3.193A5411.7 5411.7 0 015.113 12c-.668-.001-.793-.669-.36-1.003l4.68-3.881c.458-.379 1.053.26.643.688l-3.08 3.193L9.6 11z"
  }));
};

export var icon = EuiIconCloudStormy;