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

var EuiIconPagesSelect = function EuiIconPagesSelect(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    xmlns: "http://www.w3.org/2000/svg",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    fillRule: "evenodd",
    d: "M3 1a1 1 0 011-1h8a1 1 0 01.707.293l2 2A1 1 0 0115 3v5a4.995 4.995 0 00-1-.584V4h-2a1 1 0 01-1-1V1H4v12h3.1c.07.348.177.682.316 1H4a1 1 0 01-1-1V1zm5 14H2V2a1 1 0 00-1 1v12a1 1 0 001 1h7a5.029 5.029 0 01-1-1zm8-3a4 4 0 11-8 0 4 4 0 018 0zm-1.646-1.354a.5.5 0 010 .708l-2.5 2.5a.5.5 0 01-.708 0l-1-1a.5.5 0 01.708-.708l.646.647 2.146-2.147a.5.5 0 01.708 0z"
  }));
};

export var icon = EuiIconPagesSelect;