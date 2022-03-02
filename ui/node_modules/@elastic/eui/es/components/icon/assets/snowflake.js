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

var EuiIconSnowflake = function EuiIconSnowflake(_ref) {
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
    d: "M7.007.5a.5.5 0 011 0v2.024a.999.999 0 00.268-.227l1.027-1.233a.5.5 0 01.769.64L8.275 3.86a.999.999 0 01-.268.227v2.548l2.207-1.274c0-.114.02-.231.062-.346l.968-2.632a.5.5 0 11.938.345l-.554 1.506a.998.998 0 00-.062.346l1.753-1.012a.5.5 0 11.5.866l-1.753 1.012c.1.057.21.098.33.119l1.582.273a.5.5 0 11-.17.986l-2.764-.478a1 1 0 01-.33-.12L8.506 7.5l2.207 1.274a1 1 0 01.33-.119l2.764-.478a.5.5 0 11.17.986l-1.582.273a.999.999 0 00-.33.12l1.753 1.011a.5.5 0 11-.5.866l-1.753-1.012c0 .115.02.231.062.346l.554 1.506a.5.5 0 01-.938.345l-.968-2.632a.999.999 0 01-.062-.346L8.007 8.366v2.548c.098.058.19.133.268.227l1.796 2.155a.5.5 0 01-.769.64l-1.027-1.233a.999.999 0 00-.268-.226V14.5a.5.5 0 01-1 0v-2.024a.999.999 0 00-.269.227l-1.027 1.233a.5.5 0 01-.768-.64l1.795-2.155a.999.999 0 01.269-.227V8.366L4.8 9.64c0 .114-.02.231-.062.346l-.969 2.632a.5.5 0 11-.938-.345l.554-1.506a1 1 0 00.062-.346l-1.753 1.012a.5.5 0 01-.5-.866l1.753-1.012a.999.999 0 00-.33-.119l-1.582-.273a.5.5 0 01.17-.986l2.764.478c.12.02.232.062.33.12L6.508 7.5 4.3 6.226a.999.999 0 01-.33.119l-2.765.478a.5.5 0 11-.17-.986l1.582-.273a.999.999 0 00.33-.12L1.194 4.434a.5.5 0 11.5-.866l1.753 1.012c0-.114-.02-.231-.062-.346L2.83 2.727a.5.5 0 11.938-.345l.969 2.632a.999.999 0 01.062.346l2.207 1.274V4.086a1 1 0 01-.269-.227L4.943 1.704a.5.5 0 01.768-.64l1.027 1.233c.079.094.17.17.269.227V.5z"
  }));
};

export var icon = EuiIconSnowflake;