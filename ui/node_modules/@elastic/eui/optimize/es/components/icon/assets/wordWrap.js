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

var EuiIconWordWrap = function EuiIconWordWrap(_ref) {
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
    d: "M2 3h12v1H2V3zm0 8h6v1H2v-1z"
  }), ___EmotionJSX("path", {
    d: "M2 7h9.5v.5V7h.039l.083.005a2.958 2.958 0 011.102.298c.309.154.633.394.88.763.248.373.396.847.396 1.434 0 .588-.148 1.061-.396 1.434a2.257 2.257 0 01-.88.763 2.957 2.957 0 01-1.185.302h-.025l-.009.001h-.003s-.002 0-.002-.5v.5H11v1l-2-1.5 2-1.5v1h.506l.044-.003a1.959 1.959 0 00.726-.195c.191-.095.367-.23.495-.423.127-.19.229-.466.229-.879s-.102-.689-.229-.879a1.256 1.256 0 00-.495-.424 1.958 1.958 0 00-.77-.197H2V7z"
  }));
};

export var icon = EuiIconWordWrap;