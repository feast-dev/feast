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

var EuiIconTokenFlattened = function EuiIconTokenFlattened(_ref) {
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
    d: "M3.25 3a.25.25 0 00-.25.25v2c0 .138.112.25.25.25h9.5a.25.25 0 00.25-.25v-2a.25.25 0 00-.25-.25h-9.5zm0 3.75A.25.25 0 003 7v2c0 .138.112.25.25.25H5.5A.25.25 0 005.75 9V7a.25.25 0 00-.25-.25H3.25zm-.25 4a.25.25 0 01.25-.25H5.5a.25.25 0 01.25.25v2a.25.25 0 01-.25.25H3.25a.25.25 0 01-.25-.25v-2zm3.31-.727c-.082-.073-.082-.224 0-.296l3.054-2.683a.17.17 0 01.19-.026c.064.032.104.1.104.174v1.341l3.161-.016c.1 0 .18.086.18.192v2.3c0 .105-.08.191-.18.191l-3.161.017v1.341c0 .074-.04.142-.103.174a.17.17 0 01-.19-.025L6.31 10.023z"
  }));
};

export var icon = EuiIconTokenFlattened;