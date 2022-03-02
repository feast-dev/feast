/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { EuiTabbedContent } from '../../../tabs';
import { EuiText } from '../../../text';
import { EuiButton } from '../../../button';
import { EuiAbsoluteTab } from './absolute_tab';
import { EuiRelativeTab } from './relative_tab';
import { getDateMode, DATE_MODES, toAbsoluteString, toRelativeString } from '../date_modes';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiDatePopoverContent = function EuiDatePopoverContent(_ref) {
  var value = _ref.value,
      _ref$roundUp = _ref.roundUp,
      roundUp = _ref$roundUp === void 0 ? false : _ref$roundUp,
      onChange = _ref.onChange,
      dateFormat = _ref.dateFormat,
      timeFormat = _ref.timeFormat,
      locale = _ref.locale,
      position = _ref.position,
      utcOffset = _ref.utcOffset;

  var onTabClick = function onTabClick(selectedTab) {
    switch (selectedTab.id) {
      case DATE_MODES.ABSOLUTE:
        onChange(toAbsoluteString(value, roundUp));
        break;

      case DATE_MODES.RELATIVE:
        onChange(toRelativeString(value));
        break;
    }
  };

  var ariaLabel = "".concat(position === 'start' ? 'Start' : 'End', " date:");
  var renderTabs = [{
    id: DATE_MODES.ABSOLUTE,
    name: 'Absolute',
    content: ___EmotionJSX(EuiAbsoluteTab, {
      dateFormat: dateFormat,
      timeFormat: timeFormat,
      locale: locale,
      value: value,
      onChange: onChange,
      roundUp: roundUp,
      position: position,
      utcOffset: utcOffset
    }),
    'data-test-subj': 'superDatePickerAbsoluteTab',
    'aria-label': "".concat(ariaLabel, " Absolute")
  }, {
    id: DATE_MODES.RELATIVE,
    name: 'Relative',
    content: ___EmotionJSX(EuiRelativeTab, {
      dateFormat: dateFormat,
      locale: locale,
      value: toAbsoluteString(value, roundUp),
      onChange: onChange,
      roundUp: roundUp,
      position: position
    }),
    'data-test-subj': 'superDatePickerRelativeTab',
    'aria-label': "".concat(ariaLabel, " Relative")
  }, {
    id: DATE_MODES.NOW,
    name: 'Now',
    content: ___EmotionJSX(EuiText, {
      size: "s",
      color: "subdued",
      className: "euiDatePopoverContent__padded--large"
    }, ___EmotionJSX("p", null, "Setting the time to \"now\" means that on every refresh this time will be set to the time of the refresh."), ___EmotionJSX(EuiButton, {
      "data-test-subj": "superDatePickerNowButton",
      onClick: function onClick() {
        onChange('now');
      },
      fullWidth: true,
      size: "s",
      fill: true
    }, "Set ", position, " date and time to now")),
    'data-test-subj': 'superDatePickerNowTab',
    'aria-label': "".concat(ariaLabel, " Now")
  }];
  var initialSelectedTab = renderTabs.find(function (tab) {
    return tab.id === getDateMode(value);
  });
  return ___EmotionJSX(EuiTabbedContent, {
    className: "euiDatePopoverContent",
    tabs: renderTabs,
    autoFocus: "selected",
    initialSelectedTab: initialSelectedTab,
    onTabClick: onTabClick,
    size: "s",
    expand: true
  });
};
EuiDatePopoverContent.displayName = 'EuiDatePopoverContent';