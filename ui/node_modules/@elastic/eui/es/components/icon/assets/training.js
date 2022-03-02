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

var EuiIconTraining = function EuiIconTraining(_ref) {
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
    d: "M10.386 9.836a2.5 2.5 0 113.611.667C15.212 11.173 16 12.46 16 14v1.5a.5.5 0 11-1 0V14c0-1.724-1.276-3-3-3-.91 0-1.298-.02-1.805-.122-1.25-.254-2.333-1-3.585-2.566a.5.5 0 11.78-.624c.9 1.124 1.653 1.74 2.434 2.043.155.052.345.083.562.105zm1.785.128c.083.01.167.021.251.034L12.5 10a1.5 1.5 0 10-.33-.036zM9.78 11.97a.5.5 0 01.5.5c0 .076-.047.226-.05.231-.179.38-.23.774-.23 1.302v1.5a.5.5 0 11-1 0v-1.5c0-.657.072-1.186.307-1.696a.5.5 0 01.473-.337zM5.958 5.772a.5.5 0 01-.78.625L3.11 3.812a.5.5 0 11.78-.624l2.068 2.584zM1 11h5.5a.5.5 0 110 1h-6a.5.5 0 01-.5-.5V.5A.5.5 0 01.5 0h12a.5.5 0 01.5.5v3a.5.5 0 11-1 0V1H1v10z"
  }));
};

export var icon = EuiIconTraining;