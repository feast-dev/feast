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
import React, { useCallback } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { useGeneratedHtmlId } from '../../../services/accessibility';
import { EuiIcon } from '../../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiSwitch = function EuiSwitch(_ref) {
  var label = _ref.label,
      id = _ref.id,
      checked = _ref.checked,
      disabled = _ref.disabled,
      compressed = _ref.compressed,
      onChange = _ref.onChange,
      className = _ref.className,
      _ref$showLabel = _ref.showLabel,
      showLabel = _ref$showLabel === void 0 ? true : _ref$showLabel,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'button' : _ref$type,
      labelProps = _ref.labelProps,
      rest = _objectWithoutProperties(_ref, ["label", "id", "checked", "disabled", "compressed", "onChange", "className", "showLabel", "type", "labelProps"]);

  var switchId = useGeneratedHtmlId({
    conditionalId: id
  });
  var labelId = useGeneratedHtmlId({
    conditionalId: labelProps === null || labelProps === void 0 ? void 0 : labelProps.id
  });
  var onClick = useCallback(function (e) {
    if (disabled) {
      return;
    }

    var event = e;
    event.target.checked = !checked;
    onChange(event);
  }, [checked, disabled, onChange]);
  var classes = classNames('euiSwitch', {
    'euiSwitch--compressed': compressed
  }, className);
  var labelClasses = classNames('euiSwitch__label', labelProps === null || labelProps === void 0 ? void 0 : labelProps.className);

  if (showLabel === false && typeof label !== 'string') {
    console.warn('EuiSwitch `label` must be a string when `showLabel` is false.');
  }

  return ___EmotionJSX("div", {
    className: classes
  }, ___EmotionJSX("button", _extends({
    id: switchId,
    "aria-checked": checked || false,
    className: "euiSwitch__button",
    role: "switch",
    type: type,
    disabled: disabled,
    onClick: onClick,
    "aria-label": showLabel ? undefined : label,
    "aria-labelledby": showLabel ? labelId : undefined
  }, rest), ___EmotionJSX("span", {
    className: "euiSwitch__body"
  }, ___EmotionJSX("span", {
    className: "euiSwitch__thumb"
  }), ___EmotionJSX("span", {
    className: "euiSwitch__track"
  }, !compressed && ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiIcon, {
    type: "cross",
    size: "m",
    className: "euiSwitch__icon"
  }), ___EmotionJSX(EuiIcon, {
    type: "check",
    size: "m",
    className: "euiSwitch__icon euiSwitch__icon--checked"
  }))))), showLabel && // <button> + <label> has poor screen reader support.
  // Click handler added to simulate natural, secondary <label> interactivity.
  // eslint-disable-next-line jsx-a11y/no-noninteractive-element-interactions
  ___EmotionJSX("span", _extends({}, labelProps, {
    className: labelClasses,
    id: labelId,
    onClick: onClick
  }), label));
};
EuiSwitch.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Whether to render the render the text label
       */
  showLabel: PropTypes.bool,

  /**
       * Must be a string if `showLabel` prop is false
       */
  label: PropTypes.oneOfType([PropTypes.node.isRequired, PropTypes.string.isRequired]).isRequired,
  checked: PropTypes.bool.isRequired,
  onChange: PropTypes.func.isRequired,
  disabled: PropTypes.bool,
  compressed: PropTypes.bool,
  type: PropTypes.oneOf(["submit", "reset", "button"]),

  /**
       * Object of props passed to the label's <span/>
       */
  labelProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  })
};