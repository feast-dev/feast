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

var EuiIconLogoGcpMono = function EuiIconLogoGcpMono(_ref) {
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
    d: "M20.256 15.982c0-2.316-1.91-4.194-4.268-4.194-2.357 0-4.268 1.878-4.268 4.194 0 2.317 1.911 4.195 4.268 4.195 2.357 0 4.268-1.878 4.268-4.195"
  }), ___EmotionJSX("path", {
    d: "M29.87 16.543a.862.862 0 01-1.167.308.832.832 0 01-.312-1.147.862.862 0 011.167-.308.832.832 0 01.312 1.147M22.56 28.15a.847.847 0 01-.855-.84c0-.464.383-.84.855-.84s.854.376.854.84c0 .464-.382.84-.854.84m-12.715-.113a.863.863 0 01-1.167-.308.832.832 0 01.312-1.147.862.862 0 011.167.308.832.832 0 01-.312 1.147M3.586 16.542a.863.863 0 01-1.167.308.832.832 0 01-.313-1.147.862.862 0 011.167-.308.832.832 0 01.313 1.147M22.987 5.665a.862.862 0 01-1.167-.308.832.832 0 01.312-1.147.863.863 0 011.168.308.832.832 0 01-.313 1.147m-13.57.112a.847.847 0 01-.854-.84c0-.464.382-.84.854-.84s.855.376.855.84c0 .464-.383.84-.855.84m22.178 8.797l-3.697-6.292c-.014-.023-.03-.043-.045-.065l-2.83-4.818A2.854 2.854 0 0022.56 2H9.417a2.853 2.853 0 00-2.464 1.398L3.257 9.69l3.284 5.59 4.519-7.69h15.605c.491 0 .969.251 1.232.699h-6.565l4.513 7.682-4.935 8.4-2.87 4.883a1.426 1.426 0 01-1.23.699l3.279-5.582h-9.03l-4.544-7.738-.384-.653-2.874-4.893a1.377 1.377 0 01-.003-1.391L.382 14.584a2.754 2.754 0 000 2.796l6.57 11.186a2.854 2.854 0 002.465 1.398h7.392c.054 0 .107-.005.16-.011h5.59a2.853 2.853 0 002.464-1.397l6.572-11.186c.526-.896.49-1.96 0-2.796"
  }));
};

export var icon = EuiIconLogoGcpMono;