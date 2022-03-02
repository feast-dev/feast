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

var EuiIconAppFilebeat = function EuiIconAppFilebeat(_ref) {
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
    d: "M8 18h16v2H8zM8 13h9v2H8zM8 23h16v2H8z"
  }), ___EmotionJSX("path", {
    d: "M21.41 0H5a3 3 0 00-3 3v26a3 3 0 003 3h22a3 3 0 003-3V8.59L21.41 0zM22 3.41L26.59 8H22V3.41zM27 30H5a1 1 0 01-1-1V3a1 1 0 011-1h15v8h8v19a1 1 0 01-1 1z"
  }));
};

export var icon = EuiIconAppFilebeat;