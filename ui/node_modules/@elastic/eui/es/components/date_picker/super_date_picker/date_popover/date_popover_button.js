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
// eslint-disable-line import/named
import { useEuiI18n } from '../../../i18n';
import { EuiPopover } from '../../../popover';
import { formatTimeString } from '../pretty_duration';
import { EuiDatePopoverContent } from './date_popover_content';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiDatePopoverButton = function EuiDatePopoverButton(props) {
  var position = props.position,
      isDisabled = props.isDisabled,
      isInvalid = props.isInvalid,
      needsUpdating = props.needsUpdating,
      value = props.value,
      buttonProps = props.buttonProps,
      roundUp = props.roundUp,
      onChange = props.onChange,
      locale = props.locale,
      dateFormat = props.dateFormat,
      utcOffset = props.utcOffset,
      timeFormat = props.timeFormat,
      isOpen = props.isOpen,
      onPopoverToggle = props.onPopoverToggle,
      onPopoverClose = props.onPopoverClose,
      compressed = props.compressed,
      rest = _objectWithoutProperties(props, ["position", "isDisabled", "isInvalid", "needsUpdating", "value", "buttonProps", "roundUp", "onChange", "locale", "dateFormat", "utcOffset", "timeFormat", "isOpen", "onPopoverToggle", "onPopoverClose", "compressed"]);

  var classes = classNames(['euiDatePopoverButton', "euiDatePopoverButton--".concat(position), {
    'euiDatePopoverButton--compressed': compressed,
    'euiDatePopoverButton-isSelected': isOpen,
    'euiDatePopoverButton-isInvalid': isInvalid,
    'euiDatePopoverButton-needsUpdating': needsUpdating,
    'euiDatePopoverButton-disabled': isDisabled
  }]);
  var formattedValue = formatTimeString(value, dateFormat, roundUp, locale);
  var title = formattedValue;
  var invalidTitle = useEuiI18n('euiDatePopoverButton.invalidTitle', 'Invalid date: {title}', {
    title: title
  });
  var outdatedTitle = useEuiI18n('euiDatePopoverButton.outdatedTitle', 'Update needed: {title}', {
    title: title
  });

  if (isInvalid) {
    title = invalidTitle;
  } else if (needsUpdating) {
    title = outdatedTitle;
  }

  var button = ___EmotionJSX("button", _extends({
    onClick: onPopoverToggle,
    className: classes,
    title: title,
    disabled: isDisabled,
    "data-test-subj": "superDatePicker".concat(position, "DatePopoverButton")
  }, buttonProps), formattedValue);

  return ___EmotionJSX(EuiPopover, _extends({
    button: button,
    isOpen: isOpen,
    closePopover: onPopoverClose,
    anchorPosition: position === 'start' ? 'downLeft' : 'downRight',
    display: "block",
    panelPaddingSize: "none"
  }, rest), ___EmotionJSX(EuiDatePopoverContent, {
    value: value,
    roundUp: roundUp,
    onChange: onChange,
    dateFormat: dateFormat,
    timeFormat: timeFormat,
    locale: locale,
    position: position,
    utcOffset: utcOffset
  }));
};
EuiDatePopoverButton.propTypes = {
  className: PropTypes.string,
  buttonProps: PropTypes.any,
  dateFormat: PropTypes.string.isRequired,
  isDisabled: PropTypes.bool,
  isInvalid: PropTypes.bool,
  isOpen: PropTypes.bool.isRequired,
  needsUpdating: PropTypes.bool,
  locale: PropTypes.any,
  onChange: PropTypes.any.isRequired,
  onPopoverClose: PropTypes.func.isRequired,
  onPopoverToggle: PropTypes.func.isRequired,
  position: PropTypes.oneOf(["start", "end"]).isRequired,
  roundUp: PropTypes.bool,
  timeFormat: PropTypes.string.isRequired,
  value: PropTypes.string.isRequired,
  utcOffset: PropTypes.number,
  compressed: PropTypes.bool
};
EuiDatePopoverButton.displayName = 'EuiDatePopoverButton';