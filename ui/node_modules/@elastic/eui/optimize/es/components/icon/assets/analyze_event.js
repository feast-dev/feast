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

var EuiIconAnalyzeEvent = function EuiIconAnalyzeEvent(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 14,
    height: 16,
    viewBox: "0 0 16 16",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    fillRule: "evenodd",
    d: "M13.924 4.013a.605.605 0 00-.228-.236L7.304.082a.607.607 0 00-.608 0L.304 3.777A.62.62 0 000 4.304v7.392c0 .217.116.418.304.527l6.392 3.695c.188.11.42.11.608 0l6.392-3.695a.609.609 0 00.304-.527V4.304a.607.607 0 00-.076-.291zM1 5.079v6.391l6 3.47 6-3.47V5.08L7.252 8.432 7 8.579l-.252-.147L1 5.079zm11.476-.852L7 1.06 1.524 4.227 7 7.42l5.476-3.194z"
  }));
};

export var icon = EuiIconAnalyzeEvent;