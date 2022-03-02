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

var EuiIconTableDensityCompact = function EuiIconTableDensityCompact(_ref) {
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
    d: "M16 3v11a2 2 0 01-2 2H2a2 2 0 01-2-2V2a2 2 0 012-2h12a2 2 0 012 2v1zm-1 0V2a1 1 0 00-1-1H2a1 1 0 00-1 1v1h14zm0 1H1v10a1 1 0 001 1h12a1 1 0 001-1V4zM4.496 7a.5.5 0 010 1H2.495a.5.5 0 010-1h2.001zm5.218 0c.158 0 .286.224.286.5s-.128.5-.286.5H6.286C6.128 8 6 7.776 6 7.5s.128-.5.286-.5h3.428zM4.496 5a.5.5 0 010 1H2.495a.5.5 0 010-1h2.001zm5.218 0c.158 0 .286.224.286.5s-.128.5-.286.5H6.286C6.128 6 6 5.776 6 5.5s.128-.5.286-.5h3.428zM4.496 9a.5.5 0 010 1H2.495a.5.5 0 010-1h2.001zm5.218 0c.158 0 .286.224.286.5s-.128.5-.286.5H6.286C6.128 10 6 9.776 6 9.5s.128-.5.286-.5h3.428zm-5.218 2a.5.5 0 010 1H2.495a.5.5 0 010-1h2.001zm5.218 0c.158 0 .286.224.286.5s-.128.5-.286.5H6.286C6.128 12 6 11.776 6 11.5s.128-.5.286-.5h3.428zm-5.218 2a.5.5 0 010 1H2.495a.5.5 0 010-1h2.001zm9-6a.5.5 0 010 1h-2.001a.5.5 0 010-1h2.001zm0-2a.5.5 0 010 1h-2.001a.5.5 0 010-1h2.001zm0 4a.5.5 0 010 1h-2.001a.5.5 0 010-1h2.001zm0 2a.5.5 0 010 1h-2.001a.5.5 0 010-1h2.001zm0 2a.5.5 0 010 1h-2.001a.5.5 0 010-1h2.001zm-3.782 0c.158 0 .286.224.286.5s-.128.5-.286.5H6.286C6.128 14 6 13.776 6 13.5s.128-.5.286-.5h3.428z"
  }));
};

export var icon = EuiIconTableDensityCompact;