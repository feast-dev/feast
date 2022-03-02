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

var EuiIconScale = function EuiIconScale(_ref) {
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
    d: "M12.5 12a.5.5 0 110 1 .5.5 0 010-1zm-2 0a.5.5 0 110 1 .5.5 0 010-1zm-2 0a.5.5 0 110 1 .5.5 0 010-1zm4-2a.5.5 0 110 1 .5.5 0 010-1zm-2 0a.5.5 0 110 1 .5.5 0 010-1zm2-1a.5.5 0 110-1 .5.5 0 010 1zm0-3a.5.5 0 110 1 .5.5 0 010-1zm-2 2a.5.5 0 110 1 .5.5 0 010-1zm-2 0a.5.5 0 110 1 .5.5 0 010-1zm0 2a.5.5 0 110 1 .5.5 0 010-1zm-2 2a.5.5 0 110 1 .5.5 0 010-1zm-2 0a.5.5 0 110 1 .5.5 0 010-1zm2-2a.5.5 0 110 1 .5.5 0 010-1zm6-6a.5.5 0 110 1 .5.5 0 010-1zm-2 2a.5.5 0 110 1 .5.5 0 010-1z"
  }));
};

export var icon = EuiIconScale;