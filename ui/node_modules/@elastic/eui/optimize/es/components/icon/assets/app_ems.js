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

var EuiIconAppEms = function EuiIconAppEms(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M3 22h3v2H1V1h23v5h-2V3H3z"
  }), ___EmotionJSX("path", {
    d: "M15.228 29c1.492-1.678 1.353-2.859.009-5.654-.049-.1-.049-.1-.097-.203-1.369-2.855-1.626-4.491-.325-6.582 2.796-4.498 9.514-2.642 14.185 2.317V10H10v19h5.228zm2.47 0H29v-6.983c-3.88-5.406-10.376-7.795-12.487-4.4-.83 1.336-.669 2.37.43 4.662l.097.2c1.352 2.814 1.731 4.63.659 6.521zM31 8v23H8V8h23z"
  }));
};

export var icon = EuiIconAppEms;