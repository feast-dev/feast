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

var EuiIconPin = function EuiIconPin(_ref) {
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
    fillRule: "evenodd",
    d: "M8 11h4.5a.5.5 0 100-1h-10a.5.5 0 000 1H7v4.25c0 .414.224.75.5.75s.5-.336.5-.75V11zM4 4h1v6H4V4zm6 0h1v6h-1V4zM4.286 2C4.08 2 4 2.063 4 2v1c0-.063.08 0 .286 0h6.428C10.92 3 11 2.937 11 3V2c0 .063-.08 0-.286 0H4.286zm0-1h6.428C11.424 1 12 1.448 12 2v1c0 .552-.576 1-1.286 1H4.286C3.576 4 3 3.552 3 3V2c0-.552.576-1 1.286-1z"
  }));
};

export var icon = EuiIconPin;