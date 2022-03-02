import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
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
EuiDatePopoverButton.displayName = 'EuiDatePopoverButton';