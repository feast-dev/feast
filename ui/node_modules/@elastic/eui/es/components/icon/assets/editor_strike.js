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

var EuiIconEditorStrike = function EuiIconEditorStrike(_ref) {
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
    d: "M10.023 10h1.274c.006.08.01.164.01.25a2.557 2.557 0 01-.883 1.949c-.284.25-.627.446-1.03.588A4.087 4.087 0 018.028 13a4.616 4.616 0 01-3.382-1.426c-.193-.259-.193-.5 0-.724.193-.223.438-.266.735-.13.343.363.748.655 1.213.876.466.22.949.33 1.449.33.637 0 1.132-.144 1.485-.433.353-.29.53-.67.53-1.14a1.72 1.72 0 00-.034-.353zM5.586 7a2.49 2.49 0 01-.294-.507 2.316 2.316 0 01-.177-.934c0-.363.076-.701.228-1.015.152-.314.363-.586.633-.816.27-.23.588-.41.955-.537A3.683 3.683 0 018.145 3c.578 0 1.112.11 1.603.33.49.221.907.508 1.25.861.16.282.16.512 0 .692-.16.18-.38.214-.662.102a3.438 3.438 0 00-.978-.669 2.914 2.914 0 00-1.213-.242c-.54 0-.973.125-1.302.375-.328.25-.492.595-.492 1.036 0 .236.046.434.14.596.092.162.217.304.374.426.157.123.329.23.515.324.119.06.24.116.362.169H5.586zM2.5 8h11a.5.5 0 110 1h-11a.5.5 0 010-1z"
  }));
};

export var icon = EuiIconEditorStrike;