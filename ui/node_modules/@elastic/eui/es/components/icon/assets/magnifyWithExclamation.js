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

var EuiIconMagnifyWithExclamation = function EuiIconMagnifyWithExclamation(_ref) {
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
    d: "M15.143 15.85l-3.872-3.872a.5.5 0 01-.034-.738 6.01 6.01 0 000-8.486 6.004 6.004 0 00-8.484 0 6.01 6.01 0 000 8.486 5.96 5.96 0 004.242 1.757.5.5 0 010 1 6.955 6.955 0 01-4.949-2.05c-2.728-2.73-2.728-7.171 0-9.9 2.728-2.73 7.168-2.73 9.898 0 2.609 2.61 2.723 6.785.342 9.532l3.565 3.564a.502.502 0 01-.708.708zM6.254 3l.277 5.063h.935L7.754 3h-1.5zm.222 7.548a.727.727 0 00.533.222.717.717 0 00.52-.22.715.715 0 00.225-.53.716.716 0 00-.222-.534.724.724 0 00-.523-.216.731.731 0 00-.533.219.719.719 0 00-.222.53c0 .205.074.38.222.529z"
  }));
};

export var icon = EuiIconMagnifyWithExclamation;