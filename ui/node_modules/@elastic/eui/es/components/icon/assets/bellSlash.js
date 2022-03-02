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

var EuiIconBellSlash = function EuiIconBellSlash(_ref) {
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
    d: "M9.5 14l-.005.15a2 2 0 01-3.972.153l-.018-.154L5.5 14h4zm5.38-12.825a.5.5 0 01.008.64l-.063.065-14 12a.5.5 0 01-.713-.695l.063-.065 14-12a.5.5 0 01.705.055zM13.036 5.51l.055.353.048.368c.066.554.103.98.145 1.724l.08 1.491c.146 2.371.402 3.053 1.136 3.053a.5.5 0 110 1c-.603 0-1.031-.146-1.34-.499H4.185l1.182-1h7.317c-.172-.644-.264-1.528-.33-2.708l-.09-1.638c-.033-.514-.066-.87-.118-1.304l-.011-.077.901-.763zM7.5 0a2 2 0 011.98 1.717c.476.113.907.27 1.292.472l-.838.71a5.728 5.728 0 00-1.591-.36 1 1 0 10-1.684 0C4.455 2.75 3.26 3.78 2.95 5.671l-.05.334-.046.347c-.08.676-.115 1.161-.176 2.347l-.014.265-.005.09-1.058.897c.018-.234.035-.488.05-.763l.077-1.427c.038-.626.073-1.025.134-1.528.03-.25.063-.49.102-.724.334-2.04 1.546-3.313 3.555-3.792l.004-.019A2 2 0 017.5 0z"
  }));
};

export var icon = EuiIconBellSlash;