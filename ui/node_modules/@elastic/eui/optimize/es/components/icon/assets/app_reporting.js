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

var EuiIconAppReporting = function EuiIconAppReporting(_ref) {
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
    d: "M25 5h-.17v2H25a1 1 0 011 1v20a1 1 0 01-1 1H7a1 1 0 01-1-1V8a1 1 0 011-1h.17V5H7a3 3 0 00-3 3v20a3 3 0 003 3h18a3 3 0 003-3V8a3 3 0 00-3-3z"
  }), ___EmotionJSX("path", {
    d: "M23 3h-3V0h-8v3H9v6h14V3zm-2 4H11V5h3V2h4v3h3v2z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M10 13h12v2H10zM10 18h12v2H10zM10 23h12v2H10z"
  }));
};

export var icon = EuiIconAppReporting;