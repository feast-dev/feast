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

var EuiIconLettering = function EuiIconLettering(_ref) {
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
    fillRule: "evenodd",
    d: "M5 3l3 9H7L6 9H3l-1 3H1l3-9h1zm-.5 1.5L5.667 8H3.333L4.5 4.5zm7.99 1.647C12.16 6.005 11.76 6 11.5 6c-.359 0-1.022.175-1.632.838l-.736-.676C9.929 5.294 10.859 5 11.5 5h.016c.25 0 .836 0 1.369.228.281.12.568.313.782.617.216.307.333.693.333 1.155v5h-1v-.354a2.101 2.101 0 01-.064.038c-.554.317-1.166.316-1.42.316h-.032c-.25 0-.836 0-1.368-.228a1.81 1.81 0 01-.783-.617C9.117 10.848 9 10.462 9 10c0-.462.117-.848.333-1.155.214-.304.5-.496.783-.617C10.648 8 11.233 8 11.484 8h.016c.258 0 .69-.003 1.05-.106a.889.889 0 00.364-.179c.053-.05.086-.108.086-.215V7c0-.288-.07-.465-.151-.58a.814.814 0 00-.358-.273zM13 8.8c-.06.022-.118.04-.175.057C12.32 9 11.762 9 11.513 9H11.5c-.259 0-.66.005-.99.147a.814.814 0 00-.359.274c-.08.114-.151.291-.151.579s.07.465.151.58a.813.813 0 00.358.273c.331.142.732.147.991.147.257 0 .63-.008.94-.184.255-.146.56-.463.56-1.316v-.701z"
  }));
};

export var icon = EuiIconLettering;