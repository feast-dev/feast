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
import React, { forwardRef } from 'react';
import PropTypes from "prop-types";
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
EuiMarkdownEditorTextArea.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  isInvalid: PropTypes.bool,
  fullWidth: PropTypes.bool,
  compressed: PropTypes.bool,
  height: PropTypes.string.isRequired,
  maxHeight: PropTypes.string.isRequired
};
EuiMarkdownEditorTextArea.displayName = 'EuiMarkdownEditorTextArea';