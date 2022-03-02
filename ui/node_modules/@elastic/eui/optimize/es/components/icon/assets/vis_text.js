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

var EuiIconVisText = function EuiIconVisText(_ref) {
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
    d: "M8.5 5v6h2a.5.5 0 110 1h-5a.5.5 0 110-1h2V5H5v.5a.5.5 0 01-1 0v-1a.5.5 0 01.5-.5h7a.5.5 0 01.5.51l-.021 1a.5.5 0 11-1-.02l.01-.49H8.5zM1 15h1.5a.5.5 0 110 1h-2a.5.5 0 01-.5-.5v-1.996a.5.5 0 011 0V15zM1 1v1.497a.5.5 0 11-1 0V.5A.5.5 0 01.5 0h2a.5.5 0 010 1H1zm14 0h-1.495a.5.5 0 010-1H15.5a.5.5 0 01.5.5v2a.5.5 0 11-1 0V1zm0 14v-1.5a.5.5 0 111 0v2a.5.5 0 01-.5.5h-2a.5.5 0 110-1H15zM0 6.5a.5.5 0 011 0v3a.5.5 0 01-1 0v-3zM9.5 0a.5.5 0 010 1h-3a.5.5 0 010-1h3zM15 6.5a.5.5 0 111 0v3a.5.5 0 11-1 0v-3zM9.5 15a.5.5 0 110 1h-3a.5.5 0 110-1h3z"
  }));
};

export var icon = EuiIconVisText;