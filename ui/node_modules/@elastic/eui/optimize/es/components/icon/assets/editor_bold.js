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

var EuiIconEditorBold = function EuiIconEditorBold(_ref) {
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
    d: "M8.193 13H4V3h4.151c1.816 0 2.987.977 2.987 2.495 0 1.074-.797 2.01-1.823 2.176v.055c1.359.132 2.308 1.11 2.308 2.433 0 1.76-1.296 2.841-3.43 2.841zM5.788 4.393v2.82h1.635c1.248 0 1.948-.526 1.948-1.455 0-.873-.603-1.365-1.67-1.365H5.788zm0 7.214h1.996c1.316 0 2.016-.547 2.016-1.573 0-1.019-.72-1.552-2.092-1.552h-1.92v3.125z"
  }));
};

export var icon = EuiIconEditorBold;