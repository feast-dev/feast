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

var EuiIconEditorUnderline = function EuiIconEditorUnderline(_ref) {
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
    d: "M5 3.536V7.8c0 1.628 1.224 2.6 3 2.6 1.783 0 3-.972 3-2.6V3.536c0-.357.167-.536.5-.536.333 0 .5.179.5.536v4.318c0 2.093-1.665 3.546-4 3.546S4 9.893 4 7.8V3.536C4 3.179 4.167 3 4.5 3c.333 0 .5.179.5.536zM2.5 13h11a.5.5 0 110 1h-11a.5.5 0 110-1z"
  }));
};

export var icon = EuiIconEditorUnderline;