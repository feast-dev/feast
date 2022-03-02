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

var EuiIconQuote = function EuiIconQuote(_ref) {
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
    d: "M6.848 2.47a1 1 0 01-.318 1.378A7.284 7.284 0 003.75 7.01 3 3 0 111 10v-.027a3.521 3.521 0 01.01-.232c.009-.15.027-.36.062-.618.07-.513.207-1.22.484-2.014.552-1.59 1.67-3.555 3.914-4.957a1 1 0 011.378.318zm7 0a1 1 0 01-.318 1.378 7.283 7.283 0 00-2.78 3.162A3 3 0 118 10v-.027a3.521 3.521 0 01.01-.232c.009-.15.027-.36.062-.618.07-.513.207-1.22.484-2.014.552-1.59 1.67-3.555 3.914-4.957a1 1 0 011.378.318z"
  }));
};

export var icon = EuiIconQuote;