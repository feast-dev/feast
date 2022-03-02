import _extends from "@babel/runtime/helpers/extends";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useState } from 'react';
import classNames from 'classnames';
import { EuiFieldText } from '../../form';
import { EuiButtonEmpty } from '../../button/button_empty/button_empty';
import { EuiInputPopover, EuiPopover } from '../../popover';
import { useEuiI18n } from '../../i18n';
import { prettyInterval } from '../super_date_picker/pretty_interval';
import { EuiRefreshInterval } from './refresh_interval';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiAutoRefresh = function EuiAutoRefresh(_ref) {
  var className = _ref.className,
      onRefreshChange = _ref.onRefreshChange,
      isDisabled = _ref.isDisabled,
      _ref$isPaused = _ref.isPaused,
      isPaused = _ref$isPaused === void 0 ? true : _ref$isPaused,
      _ref$refreshInterval = _ref.refreshInterval,
      refreshInterval = _ref$refreshInterval === void 0 ? 1000 : _ref$refreshInterval,
      _ref$readOnly = _ref.readOnly,
      readOnly = _ref$readOnly === void 0 ? true : _ref$readOnly,
      rest = _objectWithoutProperties(_ref, ["className", "onRefreshChange", "isDisabled", "isPaused", "refreshInterval", "readOnly"]);

  var classes = classNames('euiAutoRefresh', className);

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      isPopoverOpen = _useState2[0],
      setIsPopoverOpen = _useState2[1];

  var autoRefeshLabel = useEuiI18n('euiAutoRefresh.autoRefreshLabel', 'Auto refresh');
  return ___EmotionJSX(EuiInputPopover, {
    className: classes,
    fullWidth: rest.fullWidth,
    input: ___EmotionJSX(EuiFieldText, _extends({
      "aria-label": autoRefeshLabel,
      onClick: function onClick() {
        return setIsPopoverOpen(function (isOpen) {
          return !isOpen;
        });
      },
      prepend: ___EmotionJSX(EuiButtonEmpty, {
        className: "euiFormControlLayout__prepend",
        onClick: function onClick() {
          return setIsPopoverOpen(function (isOpen) {
            return !isOpen;
          });
        },
        size: "s",
        color: "text",
        iconType: "timeRefresh",
        isDisabled: isDisabled
      }, ___EmotionJSX("strong", null, ___EmotionJSX("small", null, autoRefeshLabel))),
      readOnly: readOnly,
      disabled: isDisabled,
      value: prettyInterval(Boolean(isPaused), refreshInterval)
    }, rest)),
    isOpen: isPopoverOpen,
    closePopover: function closePopover() {
      setIsPopoverOpen(false);
    }
  }, ___EmotionJSX(EuiRefreshInterval, {
    onRefreshChange: onRefreshChange,
    isPaused: isPaused,
    refreshInterval: refreshInterval
  }));
};
export var EuiAutoRefreshButton = function EuiAutoRefreshButton(_ref2) {
  var className = _ref2.className,
      onRefreshChange = _ref2.onRefreshChange,
      isDisabled = _ref2.isDisabled,
      _ref2$isPaused = _ref2.isPaused,
      isPaused = _ref2$isPaused === void 0 ? true : _ref2$isPaused,
      _ref2$refreshInterval = _ref2.refreshInterval,
      refreshInterval = _ref2$refreshInterval === void 0 ? 1000 : _ref2$refreshInterval,
      _ref2$shortHand = _ref2.shortHand,
      shortHand = _ref2$shortHand === void 0 ? false : _ref2$shortHand,
      _ref2$size = _ref2.size,
      size = _ref2$size === void 0 ? 's' : _ref2$size,
      _ref2$color = _ref2.color,
      color = _ref2$color === void 0 ? 'text' : _ref2$color,
      rest = _objectWithoutProperties(_ref2, ["className", "onRefreshChange", "isDisabled", "isPaused", "refreshInterval", "shortHand", "size", "color"]);

  var _useState3 = useState(false),
      _useState4 = _slicedToArray(_useState3, 2),
      isPopoverOpen = _useState4[0],
      setIsPopoverOpen = _useState4[1];

  var classes = classNames('euiAutoRefreshButton', className);
  var autoRefeshLabelOff = useEuiI18n('euiAutoRefresh.buttonLabelOff', 'Auto refresh is off');
  var autoRefeshLabelOn = useEuiI18n('euiAutoRefresh.buttonLabelOn', 'Auto refresh is on and set to {prettyInterval}', {
    prettyInterval: prettyInterval(Boolean(isPaused), refreshInterval)
  });
  return ___EmotionJSX(EuiPopover, {
    button: ___EmotionJSX(EuiButtonEmpty, _extends({
      onClick: function onClick() {
        return setIsPopoverOpen(function (isOpen) {
          return !isOpen;
        });
      },
      className: classes,
      size: size,
      color: color,
      iconType: "timeRefresh",
      title: isPaused ? autoRefeshLabelOff : autoRefeshLabelOn,
      isDisabled: isDisabled
    }, rest), prettyInterval(Boolean(isPaused), refreshInterval, shortHand)),
    isOpen: isPopoverOpen,
    closePopover: function closePopover() {
      setIsPopoverOpen(false);
    }
  }, ___EmotionJSX(EuiRefreshInterval, {
    onRefreshChange: onRefreshChange,
    isPaused: isPaused,
    refreshInterval: refreshInterval
  }));
};