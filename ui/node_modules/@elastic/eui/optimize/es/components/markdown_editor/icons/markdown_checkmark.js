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
import { jsx as ___EmotionJSX } from "@emotion/react";

var MarkdownCheckmark = function MarkdownCheckmark(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: "16",
    height: "16",
    fill: "none",
    viewBox: "0 0 16 16",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    d: "M11.828 13H4.172A1.173 1.173 0 013 11.828V4.172C3 3.526 3.526 3 4.172 3h7.656C12.474 3 13 3.526 13 4.172v7.656c0 .646-.526 1.172-1.172 1.172zM4.172 3.781a.391.391 0 00-.39.39v7.657c0 .216.175.39.39.39h7.656a.39.39 0 00.39-.39V4.172a.391.391 0 00-.39-.39H4.172zm7.244 2.175l-.582-.521L7.22 9.469 5.125 7.476l-.539.566 2.68 2.548 4.15-4.634z"
  }));
};

export default MarkdownCheckmark;