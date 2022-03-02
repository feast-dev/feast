function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

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

var EuiIconPayment = function EuiIconPayment(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 16,
    height: 16,
    fill: "none",
    viewBox: "0 0 16 16",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    d: "M.586 2.586A2 2 0 000 4h1a1 1 0 011-1V2a2 2 0 00-1.414.586zM2 2h10.5a.5.5 0 010 1H2V2zM0 4h1v6.5a.5.5 0 01-1 0V4zm2.586.586A2 2 0 002 6h1a1 1 0 011-1V4a2 2 0 00-1.414.586zm0 8.828A2 2 0 012 12h1a1 1 0 001 1v1a2 2 0 01-1.414-.586zm12.828-8.828A2 2 0 0116 6h-1a1 1 0 00-1-1V4a2 2 0 011.414.586zm0 8.828A2 2 0 0016 12h-1a1 1 0 01-1 1v1a2 2 0 001.414-.586zM4 4h10v1H4zM3 7h12v1H3zm1 6h10v1H4zM2 6h1v6H2zm13 0h1v6h-1zm-5.5 4a.5.5 0 010 1H7.496a.5.5 0 010-1H9.5zm4 0a.5.5 0 010 1h-2.004a.5.5 0 010-1H13.5z"
  }));
};

export var icon = EuiIconPayment;