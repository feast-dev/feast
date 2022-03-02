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

var EuiIconTokenRepo = function EuiIconTokenRepo(_ref) {
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
    d: "M8.533 9.067c-1.792 0-2.378.72-2.57 1.194a1.601 1.601 0 11-1.163-.037V5.776a1.595 1.595 0 01-1.067-1.51c0-.885.715-1.6 1.6-1.6.886 0 1.6.715 1.6 1.6 0 .7-.442 1.291-1.066 1.51v2.821C6.336 8.251 7.019 8 8 8c1.424 0 1.899-.715 2.053-1.19a1.603 1.603 0 01-.986-1.477c0-.885.714-1.6 1.6-1.6.885 0 1.6.715 1.6 1.6a1.59 1.59 0 01-1.115 1.526c-.139.762-.656 2.208-2.619 2.208zm-3.2 2.133a.535.535 0 00-.533.533c0 .294.24.534.533.534a.535.535 0 000-1.067zm0-7.467a.535.535 0 00-.533.534c0 .293.24.533.533.533.294 0 .534-.24.534-.533a.535.535 0 00-.534-.534zM10.667 4.8a.535.535 0 00-.534.533.535.535 0 001.067 0 .535.535 0 00-.533-.533z"
  }));
};

export var icon = EuiIconTokenRepo;