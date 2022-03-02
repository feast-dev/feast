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

var EuiIconAppPacketbeat = function EuiIconAppPacketbeat(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M16 20a4 4 0 110-8 4 4 0 010 8zm0-6a2 2 0 100 4 2 2 0 000-4z"
  }), ___EmotionJSX("path", {
    d: "M8 4a4 4 0 10-4 4 4 4 0 002-.57l5.27 5.27 1.41-1.41L7.43 6A4 4 0 008 4zM4 6a2 2 0 110-4 2 2 0 010 4zm16.71 6.71L26 7.43A4 4 0 0028 8a4 4 0 10-4-4 4 4 0 00.57 2l-5.27 5.27 1.41 1.44zM28 2a2 2 0 110 4 2 2 0 010-4zM11.29 19.29L6 24.57A4 4 0 004 24a4 4 0 104 4 4 4 0 00-.57-2l5.27-5.27-1.41-1.44zM4 30a2 2 0 110-4 2 2 0 010 4zm24-6a4 4 0 00-2 .57l-5.27-5.27-1.41 1.41L24.57 26a4 4 0 00-.57 2 4 4 0 104-4zm0 6a2 2 0 110-4 2 2 0 010 4z"
  }));
};

export var icon = EuiIconAppPacketbeat;