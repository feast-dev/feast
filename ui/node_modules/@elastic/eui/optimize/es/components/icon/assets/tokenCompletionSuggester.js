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

var EuiIconTokenCompletionSuggester = function EuiIconTokenCompletionSuggester(_ref) {
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
    d: "M3 4a1 1 0 011-1h8a1 1 0 011 1v5.996a1 1 0 01-1 1h-1.661L7.4 13.2A.25.25 0 017 13v-2.004H4a1 1 0 01-1-1V4zm1.5 1a.5.5 0 01.5-.5h6a.5.5 0 01.5.5v4a.5.5 0 01-.5.5h-.9L8 11V9.5H5a.5.5 0 01-.5-.5V5z",
    clipRule: "evenodd"
  }), ___EmotionJSX("path", {
    d: "M6.75 6a1 1 0 100 2 1 1 0 000-2zm2.5 0a1 1 0 100 2 1 1 0 000-2z"
  }));
};

export var icon = EuiIconTokenCompletionSuggester;