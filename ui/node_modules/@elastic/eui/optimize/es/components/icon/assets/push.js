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

var EuiIconPush = function EuiIconPush(_ref) {
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
    d: "M8.171 5.15L10.114 7H1.556C1.249 7 1 7.224 1 7.5s.249.5.556.5h8.526l-1.91 1.82a.52.52 0 000 .77c.227.213.6.213.828 0l2.05-1.95a1.552 1.552 0 000-2.31L9 4.38a.617.617 0 00-.829 0 .52.52 0 000 .77z"
  }), ___EmotionJSX("path", {
    d: "M6.804 12.792A.993.993 0 016 11.82V10H5v1.826c0 .945.673 1.76 1.608 1.945l6 1.19A1.992 1.992 0 0015 13.016V1.984A2 2 0 0012.608.04l-6 1.19C5.673 1.415 5 2.23 5 3.175V5h1V3.18c0-.472.336-.879.804-.972l6-1.189A1 1 0 0114 1.991v11.018a.995.995 0 01-1.196.972l-6-1.19z"
  }));
};

export var icon = EuiIconPush;