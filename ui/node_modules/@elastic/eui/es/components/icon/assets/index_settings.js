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

var EuiIconIndexSettings = function EuiIconIndexSettings(_ref) {
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
    d: "M5 5h5.999V4H5v1zM3 5h1V4H3v1zm0 3h1V7H3v1zm6.022-1l-.15.333-.737-.078-.467-.05-.33.342A5.13 5.13 0 006.948 8H5V7h4.022zm-3.005 3L6 10.056l.306.411.399.533H5v-1h1.017zM3 11h1v-1H3v1z"
  }), ___EmotionJSX("path", {
    d: "M13 7.05l-.162-.359-.2-.447-.47-.11A5.019 5.019 0 0012 6.098V2H2v11h4.36c.157.354.355.69.59 1H1V1h12v6.05z"
  }), ___EmotionJSX("path", {
    d: "M11.004 7c.322 0 .646.036.966.109l.595 1.293 1.465-.152c.457.462.786 1.016.969 1.61l-.87 1.14.871 1.141a3.94 3.94 0 01-.387.859 4.058 4.058 0 01-.583.75l-1.465-.152-.594 1.292a4.37 4.37 0 01-1.941.001l-.594-1.293-1.466.152a3.954 3.954 0 01-.969-1.61l.87-1.14L7 9.86a3.947 3.947 0 01.97-1.61l1.466.152.593-1.292a4.37 4.37 0 01.975-.11zM11 12a1 1 0 10.002-1.998A1 1 0 0011 12z"
  }));
};

export var icon = EuiIconIndexSettings;