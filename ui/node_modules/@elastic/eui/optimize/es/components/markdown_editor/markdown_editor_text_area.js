import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { forwardRef } from 'react';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiMarkdownEditorTextArea = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var children = _ref.children,
      compressed = _ref.compressed,
      id = _ref.id,
      isInvalid = _ref.isInvalid,
      name = _ref.name,
      placeholder = _ref.placeholder,
      rows = _ref.rows,
      height = _ref.height,
      maxHeight = _ref.maxHeight,
      rest = _objectWithoutProperties(_ref, ["children", "compressed", "id", "isInvalid", "name", "placeholder", "rows", "height", "maxHeight"]);

  return ___EmotionJSX("textarea", _extends({
    ref: ref,
    style: {
      height: height,
      maxHeight: maxHeight
    },
    className: "euiMarkdownEditorTextArea"
  }, rest, {
    rows: 6,
    name: name,
    id: id,
    placeholder: placeholder
  }), children);
});
EuiMarkdownEditorTextArea.displayName = 'EuiMarkdownEditorTextArea';