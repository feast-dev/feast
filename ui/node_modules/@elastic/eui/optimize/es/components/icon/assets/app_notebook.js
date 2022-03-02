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

var EuiIconAppNotebook = function EuiIconAppNotebook(_ref) {
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
    d: "M25 2h-5V0h-2v2h-3V0h-2v2h-3V0H8v2H3v26h22V2zm-2 24H5V4h3v2h2V4h3v2h2V4h3v2h2V4h3v22z"
  }), ___EmotionJSX("path", {
    d: "M27 7v23H8v2h21V7z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M8 12h12v2H8zM8 17h12v2H8z"
  }));
};

export var icon = EuiIconAppNotebook;