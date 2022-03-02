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

var EuiIconTimeRefresh = function EuiIconTimeRefresh(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    fill: "none",
    xmlns: "http://www.w3.org/2000/svg",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    clipRule: "evenodd",
    d: "M12.809 8.807a4.5 4.5 0 01-5.572-5.75A5 5 0 003.04 9.31a.5.5 0 01-.966.259 6 6 0 015.672-7.553 4.5 4.5 0 116.117 6.313 6.002 6.002 0 01-7.803 5.409.5.5 0 11.303-.954 5.001 5.001 0 006.445-3.979zM11.859 5H13.5a.5.5 0 000-1H12V1.5a.5.5 0 00-1 0v3a.5.5 0 00.5.5h.36z"
  }), ___EmotionJSX("path", {
    d: "M3.757 15.848l-.776-2.897a1.5 1.5 0 011.06-1.838l2.898-.776a.5.5 0 11.259.966l-2.898.776a.5.5 0 00-.353.613l.776 2.898a.5.5 0 01-.966.258z"
  }));
};

export var icon = EuiIconTimeRefresh;