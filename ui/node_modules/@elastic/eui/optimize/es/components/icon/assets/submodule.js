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

var EuiIconSubmodule = function EuiIconSubmodule(_ref) {
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
    d: "M6 2H1v12h3V7a1 1 0 011-1h5a1 1 0 011 1v1h4V4H7c-.621 0-1-.379-1-1V2zm10 6v6a1 1 0 01-1 1H1a1 1 0 01-1-1V2a1 1 0 011-1h5.25a.75.75 0 01.75.75l-.004.206C6.99 2.317 6.974 3 7 3h8a1 1 0 011 1v4zm-1 1h-4a1 1 0 01-1-1V7H5v7h10V9zM2 4.5a.5.5 0 01.5-.5h2a.5.5 0 010 1h-2a.5.5 0 01-.5-.5zM6.5 9a.5.5 0 000 1h2a.5.5 0 000-1h-2z"
  }));
};

export var icon = EuiIconSubmodule;