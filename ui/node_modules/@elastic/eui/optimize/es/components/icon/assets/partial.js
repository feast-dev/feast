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

var EuiIconPartial = function EuiIconPartial(_ref) {
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
    d: "M6.664 14.871a7 7 0 01-2.42-12.778.5.5 0 01.612.06c.456.431 8.216 8.212 8.98 9.002a.5.5 0 01.063.618 7.002 7.002 0 01-7.235 3.098zm6.168-3.312a1961.733 1961.733 0 00-8.377-8.4 6 6 0 108.378 8.4zm2.095-2.548a.5.5 0 11-.99-.144c.01-.066.01-.066.018-.133a6.007 6.007 0 00-.034-1.714.5.5 0 11.987-.163c.108.655.122 1.326.04 1.999l-.021.155zm-1.273-5.138a.5.5 0 11-.808.59 6.026 6.026 0 00-1.304-1.308.5.5 0 01.59-.806 7.026 7.026 0 011.522 1.524zM9.169 1.098a.5.5 0 11-.166.986 6.105 6.105 0 00-1.849-.026.5.5 0 01-.14-.99 7.02 7.02 0 012.155.03z"
  }));
};

export var icon = EuiIconPartial;