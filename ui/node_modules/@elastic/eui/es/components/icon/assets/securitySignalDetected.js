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

var EuiIconSecuritySignalDetected = function EuiIconSecuritySignalDetected(_ref) {
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
    d: "M13.657 3.05a.5.5 0 00-.707-.707l-.366.366A7 7 0 108 15a4.994 4.994 0 01-.597-1.03 6 6 0 114.471-10.552l-.71.71a5 5 0 10-4.08 8.788 5.027 5.027 0 01-.082-1.042A4.002 4.002 0 018 4a3.98 3.98 0 012.453.84l-.715.714a3 3 0 00-3.86 4.567.5.5 0 10.708-.707 2 2 0 012.43-3.137l-.757.757a1 1 0 10.707.707l1.155-1.155 2.46-2.46a5.972 5.972 0 011.39 3.277c.367.158.713.36 1.029.597 0-1.636-.57-3.271-1.71-4.584l.367-.366zM16 12a4 4 0 11-8 0 4 4 0 018 0zm-4 .5a.577.577 0 01-.57-.495l-.29-2.015a.867.867 0 111.718 0l-.288 2.015a.577.577 0 01-.57.495zm0 2.5a1 1 0 100-2 1 1 0 000 2z"
  }));
};

export var icon = EuiIconSecuritySignalDetected;