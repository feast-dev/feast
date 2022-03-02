import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useCallback } from 'react';
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