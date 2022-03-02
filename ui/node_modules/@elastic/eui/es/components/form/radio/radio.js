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
import React from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiRadio = function EuiRadio(_ref) {
  var className = _ref.className,
      id = _ref.id,
      name = _ref.name,
      checked = _ref.checked,
      label = _ref.label,
      value = _ref.value,
      onChange = _ref.onChange,
      disabled = _ref.disabled,
      compressed = _ref.compressed,
      autoFocus = _ref.autoFocus,
      labelProps = _ref.labelProps,
      rest = _objectWithoutProperties(_ref, ["className", "id", "name", "checked", "label", "value", "onChange", "disabled", "compressed", "autoFocus", "labelProps"]);

  var classes = classNames('euiRadio', {
    'euiRadio--noLabel': !label,
    'euiRadio--compressed': compressed
  }, className);
  var labelClasses = classNames('euiRadio__label', labelProps === null || labelProps === void 0 ? void 0 : labelProps.className);
  var optionalLabel;

  if (label) {
    optionalLabel = ___EmotionJSX("label", _extends({}, labelProps, {
      className: labelClasses,
      htmlFor: id
    }), label);
  }

  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), ___EmotionJSX("input", {
    className: "euiRadio__input",
    type: "radio",
    id: id,
    name: name,
    value: value,
    checked: checked,
    onChange: onChange,
    disabled: disabled,
    autoFocus: autoFocus
  }), ___EmotionJSX("div", {
    className: "euiRadio__circle"
  }), optionalLabel);
};
EuiRadio.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  autoFocus: PropTypes.bool,

  /**
     * When `true` creates a shorter height radio row
     */

  /**
     * When `true` creates a shorter height radio row
     */

  /**
     * When `true` creates a shorter height radio row
     */
  compressed: PropTypes.bool,
  name: PropTypes.string,
  value: PropTypes.string,
  checked: PropTypes.bool,
  disabled: PropTypes.bool,
  onChange: PropTypes.any.isRequired,

  /**
     * Object of props passed to the <label/>
     */

  /**
     * Object of props passed to the <label/>
     */

  /**
     * Object of props passed to the <label/>
     */
  labelProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }),
  label: PropTypes.node,
  id: PropTypes.oneOfType([PropTypes.string, PropTypes.string.isRequired])
};