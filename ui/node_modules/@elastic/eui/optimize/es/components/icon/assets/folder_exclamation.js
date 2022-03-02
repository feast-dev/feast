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

var EuiIconFolderExclamation = function EuiIconFolderExclamation(_ref) {
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
    d: "M1 9.5l.826-3.717A1 1 0 012.802 5H13V4H7.125A1.125 1.125 0 016 2.875V2H1v7.5zm.247 3.5H7.1c.07.348.177.682.316 1H1a1 1 0 01-1-1V2a1 1 0 011-1h5.25a.75.75 0 01.75.75v1.125c0 .069.056.125.125.125H13a1 1 0 011 1v1h.753a1 1 0 01.977 1.217l-.447 2.011a5.015 5.015 0 00-.887-.618L14.753 6H2.803l-1.556 7zM16 12a4 4 0 11-8 0 4 4 0 018 0zm-4 .5a.577.577 0 01-.57-.495l-.29-2.015a.867.867 0 111.718 0l-.288 2.015a.577.577 0 01-.57.495zm0 2.5a1 1 0 100-2 1 1 0 000 2z"
  }));
};

export var icon = EuiIconFolderExclamation;