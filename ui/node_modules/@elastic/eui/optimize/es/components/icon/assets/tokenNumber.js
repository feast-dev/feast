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

var EuiIconTokenNumber = function EuiIconTokenNumber(_ref) {
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
    d: "M7.8 9.812h-.842l-.78 2.335H5.031l.78-2.335H4.6l.182-1.043h1.373l.507-1.504H5.454l.188-1.042h1.367l.792-2.37H8.94l-.792 2.37h.849l.792-2.37h1.145l-.792 2.37H11.4l-.182 1.042H9.8L9.293 8.77h1.248l-.183 1.043H8.946l-.775 2.335H7.026L7.8 9.812zm-.5-1.043h.842l.513-1.504h-.849L7.3 8.77z"
  }));
};

export var icon = EuiIconTokenNumber;