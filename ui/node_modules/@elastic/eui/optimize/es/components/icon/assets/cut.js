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

var EuiIconCut = function EuiIconCut(_ref) {
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
    fillRule: "evenodd",
    d: "M5.142 11.074l-1.912.548a2.532 2.532 0 11-1.395-4.867l1.947-.559a2.532 2.532 0 012.555.713l1.53-5.335c.139-.485.6-.897 1.159-1.238.27-.164.52-.278.779-.32.814-.132 1.503.558 1.261 1.422L9.574 6.643l4.988-1.43c.864-.242 1.554.447 1.422 1.26-.042.26-.156.51-.32.78-.341.56-.753 1.02-1.238 1.16L9.523 9.817a2.53 2.53 0 01.56 2.4l-.56 1.947a2.532 2.532 0 11-4.867-1.395l.486-1.696zm.33-1.148l.48-1.673a1.52 1.52 0 00-1.89-1.083l-1.948.558a1.52 1.52 0 00.837 2.92l2.52-.722zm3.773-2.135l-.33 1.148 5.232-1.5c.324-.093 1.182-1.39.694-1.253L9.245 7.791zM5.63 13.049a1.52 1.52 0 002.92.837l.559-1.947a1.52 1.52 0 00-1.553-1.935l2.537-8.845c.136-.488-1.16.37-1.253.694L5.63 13.05zm.973.279l.559-1.947a.506.506 0 11.973.279l-.558 1.947a.506.506 0 11-.974-.28zm-3.93-3.653a.506.506 0 11-.28-.973l1.947-.558a.506.506 0 01.28.973l-1.948.558z"
  }));
};

export var icon = EuiIconCut;