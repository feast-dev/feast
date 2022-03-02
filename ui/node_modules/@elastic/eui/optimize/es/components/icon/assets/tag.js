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

var EuiIconTag = function EuiIconTag(_ref) {
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
    d: "M6.254 14.97L.996 9.712c-.315-.316-.397-.463-.45-.64a.909.909 0 010-.534c.053-.177.135-.324.45-.64L7.43 1.466c.182-.183.252-.24.338-.293a.87.87 0 01.273-.113c.099-.023.188-.032.446-.032h5.173c.445 0 .607.046.77.133.162.087.29.214.377.377.088.162.134.324.136.769l.015 5.15c0 .259-.009.348-.032.448a.87.87 0 01-.112.273c-.054.087-.111.157-.294.34L8.067 14.97c-.315.315-.462.396-.639.45a.909.909 0 01-.535 0c-.176-.054-.324-.135-.639-.45zm1.106-.707l6.453-6.453c.092-.092.126-.128.141-.147.003-.025.004-.074.004-.204l-.015-5.15c0-.181-.003-.245-.009-.275a2.247 2.247 0 00-.274-.007H8.487c-.13 0-.179.001-.203.004-.02.015-.055.05-.147.141L1.703 8.606a2.248 2.248 0 00-.189.2c.017.024.061.07.19.198l5.257 5.259c.128.128.175.171.2.188.024-.017.071-.06.2-.188zm4.972-10.607a2 2 0 11-2.828 2.828 2 2 0 012.828-2.828zm-.707.707a1 1 0 10-1.414 1.414 1 1 0 001.414-1.414zM6.807 11.28L4.686 9.159a.5.5 0 11.707-.707l2.121 2.12a.5.5 0 11-.707.708zm1.414-1.414l-2.12-2.122a.5.5 0 11.706-.707L8.928 9.16a.5.5 0 11-.707.707z"
  }));
};

export var icon = EuiIconTag;