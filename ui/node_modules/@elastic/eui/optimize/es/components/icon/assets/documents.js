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

var EuiIconDocuments = function EuiIconDocuments(_ref) {
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
    d: "M8.8 0c.274 0 .537.113.726.312l3.2 3.428c.176.186.274.433.274.689V13a1 1 0 01-1 1H2a1 1 0 01-1-1V1a1 1 0 011-1h6.8zM12 5H8.5a.5.5 0 01-.5-.5V1H2v12h10V5zm-7.5 6a.5.5 0 110-1h5a.5.5 0 110 1h-5zm0-3a.5.5 0 010-1h5a.5.5 0 110 1h-5zm1 8a.5.5 0 110-1H14V6.5a.5.5 0 111 0V15a1 1 0 01-1 1H5.5z"
  }));
};

export var icon = EuiIconDocuments;