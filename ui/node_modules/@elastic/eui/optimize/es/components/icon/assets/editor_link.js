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

var EuiIconEditorLink = function EuiIconEditorLink(_ref) {
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
    d: "M7.556 5.051a.45.45 0 00.637.637l1.503-1.504c.432-.431 1.278-.382 1.89.23.612.612.662 1.458.23 1.89L9.519 8.6c-.432.432-1.278.383-1.89-.23a.45.45 0 10-.636.637c.914.914 2.33 1.063 3.162.23l2.297-2.297c.833-.833.684-2.248-.23-3.162-.914-.915-2.33-1.063-3.162-.23L7.556 5.051zm.888 5.261a.45.45 0 00-.637 0l-1.503 1.504c-.432.431-1.278.382-1.89-.23-.612-.612-.661-1.458-.23-1.89L6.481 7.4c.432-.432 1.278-.383 1.89.23a.45.45 0 00.636-.637c-.914-.914-2.33-1.063-3.162-.23L3.548 9.06c-.833.833-.685 2.248.23 3.162.914.915 2.33 1.063 3.162.23l1.504-1.503a.45.45 0 000-.637zM7.877 5.76a.39.39 0 00.274-.114l1.503-1.504-1.503 1.504a.39.39 0 01-.274.114zm.912 3.183c-.4.003-.843-.172-1.202-.53.359.358.802.533 1.202.53zM12.18 3.82c-.502-.503-1.155-.766-1.773-.76.618-.006 1.27.257 1.773.76.898.898 1.034 2.275.23 3.078l-2.297 2.297 2.297-2.297c.804-.803.668-2.18-.23-3.078zm-4.062 6.42a.39.39 0 01.284.667L6.898 12.41l1.504-1.503a.39.39 0 00-.284-.667zm-.926-3.965c.618-.006 1.27.257 1.773.76-.502-.503-1.155-.766-1.773-.76zM5.414 12.15a1.762 1.762 0 01-1.042-.522c-.626-.627-.692-1.511-.23-1.974L6.44 7.358 4.142 9.654c-.462.463-.396 1.348.23 1.974.311.311.687.484 1.042.522z"
  }));
};

export var icon = EuiIconEditorLink;