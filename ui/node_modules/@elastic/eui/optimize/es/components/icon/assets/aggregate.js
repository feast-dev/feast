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

var EuiIconAggregate = function EuiIconAggregate(_ref) {
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
    d: "M2.5 2a.5.5 0 100 1 .5.5 0 000-1zm0-1a1.5 1.5 0 011.415 1h1.908a1.5 1.5 0 011.393.943L8.839 7H12.5a.5.5 0 010 1H8.839l-1.623 4.057A1.5 1.5 0 015.823 13H3.915a1.5 1.5 0 110-1h1.908a.5.5 0 00.464-.314L7.761 8H3.915a1.5 1.5 0 110-1H7.76L6.287 3.314A.5.5 0 005.823 3H3.915A1.5 1.5 0 112.5 1zm0 11a.5.5 0 110 1 .5.5 0 010-1zM3 7.5a.5.5 0 10-1 0 .5.5 0 001 0zm9.354-3.354a.5.5 0 00-.708.708L13.793 7a.707.707 0 010 1l-2.147 2.146a.5.5 0 00.708.708L14.5 8.707a1.707 1.707 0 000-2.414l-2.146-2.147z"
  }));
};

export var icon = EuiIconAggregate;