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

var EuiIconReporter = function EuiIconReporter(_ref) {
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
    d: "M7.684.895L6.074.358a1 1 0 00-1.296.753L4.2 4H2.5a.5.5 0 000 1h1.626a4.007 4.007 0 00.11 2.359l-2.072-.345A1 1 0 001 8v1c.364 0 .706.097 1 .268V8l1 .167 1.859.31 2.163.36.478.08v6L2 14v-1.268A1.99 1.99 0 011 13v1a1 1 0 00.836.986l6 1c.108.018.22.018.328 0l6-1A1 1 0 0015 14v-1a1.99 1.99 0 01-1-.268V14l-5.5.917v-6l.478-.08 2.163-.36L13 8.166 14 8v1.268A1.99 1.99 0 0115 9V8a1 1 0 00-1.164-.986l-2.073.345A3.991 3.991 0 0011.874 5H13.5a.5.5 0 000-1h-1.7l-.578-2.89A1 1 0 009.925.359L8.316.895a1 1 0 01-.632 0zm2.88 6.664A3.013 3.013 0 0010.83 5H5.17a3.013 3.013 0 00.266 2.559L8 7.986l2.564-.427zM10.8 4H9.2L9 3l1.5-.5.3 1.5zM1 12a1 1 0 100-2 1 1 0 000 2zm14 0a1 1 0 100-2 1 1 0 000 2z"
  }));
};

export var icon = EuiIconReporter;