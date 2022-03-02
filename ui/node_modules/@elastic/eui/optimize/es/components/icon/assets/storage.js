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

var EuiIconStorage = function EuiIconStorage(_ref) {
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
  }, title) : null, ___EmotionJSX("g", {
    fillRule: "evenodd",
    transform: "translate(0 2)"
  }, ___EmotionJSX("path", {
    fillRule: "nonzero",
    d: "M2 6a1 1 0 01-1-1V2a1 1 0 011-1h12a1 1 0 011 1v3a1 1 0 01-1 1H2zm13 2v3a1 1 0 01-1 1H2a1 1 0 01-1-1V8a1 1 0 011-1h12a1 1 0 011 1zm1-3V2a2 2 0 00-2-2H2a2 2 0 00-2 2v3c0 .601.271 1.133.69 1.5C.271 6.867 0 7.399 0 8v3a2 2 0 002 2h12a2 2 0 002-2V8c0-.601-.271-1.133-.689-1.5.418-.367.689-.899.689-1.5z"
  }), ___EmotionJSX("circle", {
    cx: 4.5,
    cy: 9.5,
    r: 1.5
  }), ___EmotionJSX("circle", {
    cx: 4.5,
    cy: 3.5,
    r: 1.5
  }), ___EmotionJSX("path", {
    d: "M12 8h1v3h-1zM10 8h1v3h-1zM12 2h1v3h-1zM10 2h1v3h-1z"
  })));
};

export var icon = EuiIconStorage;