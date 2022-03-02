import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

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

var EuiIconSave = function EuiIconSave(_ref) {
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
    d: "M5.008 2H2.282c-.181 0-.245.002-.275.007-.005.03-.007.094-.007.275v11.436c0 .181.002.245.007.275.03.005.094.007.275.007h11.436c.181 0 .245-.002.275-.007.005-.03.007-.094.007-.275V4.62c0-.13-.001-.18-.004-.204a2.654 2.654 0 00-.141-.147L11.73 2.145a2.654 2.654 0 00-.147-.141A2.654 2.654 0 0011.38 2h-.388c.005.08.008.172.008.282v2.436c0 .446-.046.607-.134.77a.909.909 0 01-.378.378c-.163.088-.324.134-.77.134H6.282c-.446 0-.607-.046-.77-.134a.909.909 0 01-.378-.378C5.046 5.325 5 5.164 5 4.718V2.282c0-.11.003-.202.008-.282zM2.282 1h9.098c.259 0 .348.01.447.032a.87.87 0 01.273.113c.086.054.156.11.338.293l2.124 2.124c.182.182.239.252.293.338a.87.87 0 01.113.273c.023.1.032.188.032.447v9.098c0 .446-.046.607-.134.77a.909.909 0 01-.378.378c-.163.088-.324.134-.77.134H2.282c-.446 0-.607-.046-.77-.134a.909.909 0 01-.378-.378c-.088-.163-.134-.324-.134-.77V2.282c0-.446.046-.607.134-.77a.909.909 0 01.378-.378c.163-.088.324-.134.77-.134zM6 2.282v2.436c0 .181.002.245.007.275.03.005.094.007.275.007h3.436c.181 0 .245-.002.275-.007.005-.03.007-.094.007-.275V2.282c0-.181-.002-.245-.007-.275A2.248 2.248 0 009.718 2H6.282c-.181 0-.245.002-.275.007-.005.03-.007.094-.007.275zM8 12a2 2 0 110-4 2 2 0 010 4zm0-1a1 1 0 100-2 1 1 0 000 2z"
  }));
};

export var icon = EuiIconSave;