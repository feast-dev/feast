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

var EuiIconKqlOperand = function EuiIconKqlOperand(_ref) {
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
    d: "M11.192 10.145l2.298-1.792c.259-.196.259-.509 0-.706l-2.298-1.792c-.256-.196-.256-.513 0-.708a.81.81 0 01.93 0l2.3 1.791c.772.59.77 1.537 0 2.124l-2.3 1.791a.81.81 0 01-.93 0c-.256-.195-.256-.512 0-.708zm-6.384-4.29L2.51 7.647c-.259.196-.259.509 0 .706l2.298 1.792c.256.196.256.513 0 .708a.81.81 0 01-.93 0l-2.3-1.791c-.772-.59-.77-1.537 0-2.124l2.3-1.791a.81.81 0 01.93 0c.256.195.256.512 0 .708zM6.5 6h3a.5.5 0 010 1h-3a.5.5 0 010-1zm0 3h3a.5.5 0 010 1h-3a.5.5 0 010-1z"
  }));
};

export var icon = EuiIconKqlOperand;