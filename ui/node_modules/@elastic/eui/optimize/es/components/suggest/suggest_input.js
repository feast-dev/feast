import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/* eslint-disable @typescript-eslint/no-unused-vars */
import React, { useState } from 'react';
import classNames from 'classnames';
import { EuiFieldText } from '../form';
import { EuiToolTip } from '../tool_tip';
import { EuiIcon } from '../icon';
import { EuiInputPopover } from '../popover';
import { jsx as ___EmotionJSX } from "@emotion/react";
var statusMap = {
  unsaved: {
    icon: 'dot',
    color: 'accent',
    tooltip: 'Changes have not been saved.'
  },
  saved: {
    icon: 'checkInCircleFilled',
    color: 'success',
    tooltip: 'Saved.'
  },
  unchanged: {
    icon: '',
    color: 'success'
  },
  loading: {}
};
export var EuiSuggestInput = function EuiSuggestInput(props) {
  var _useState = useState(''),
      _useState2 = _slicedToArray(_useState, 2),
      value = _useState2[0],
      setValue = _useState2[1];

  var _useState3 = useState(false),
      _useState4 = _slicedToArray(_useState3, 2),
      isPopoverOpen = _useState4[0],
      setIsPopoverOpen = _useState4[1];

  var className = props.className,
      _props$status = props.status,
      status = _props$status === void 0 ? 'unchanged' : _props$status,
      append = props.append,
      tooltipContent = props.tooltipContent,
      suggestions = props.suggestions,
      sendValue = props.sendValue,
      rest = _objectWithoutProperties(props, ["className", "status", "append", "tooltipContent", "suggestions", "sendValue"]);

  var onFieldChange = function onFieldChange(e) {
    setValue(e.target.value);
    setIsPopoverOpen(e.target.value !== '' ? true : false);
    if (sendValue) sendValue(e.target.value);
  };

  var closePopover = function closePopover() {
    setIsPopoverOpen(false);
  };

  var icon = '';
  var color = '';

  if (statusMap[status]) {
    icon = statusMap[status].icon || '';
    color = statusMap[status].color || '';
  }

  var classes = classNames('euiSuggestInput', className); // EuiFieldText's append accepts an array of elements so start by creating an empty array

  var appendArray = [];

  var statusElement = (status === 'saved' || status === 'unsaved') && ___EmotionJSX(EuiToolTip, {
    position: "left",
    content: tooltipContent || statusMap[status].tooltip
  }, ___EmotionJSX(EuiIcon, {
    className: "euiSuggestInput__statusIcon",
    color: color,
    type: icon
  })); // Push the status element to the array if it is not undefined


  if (statusElement) appendArray.push(statusElement); // Check to see if consumer passed an append item and if so, add it to the array

  if (append) appendArray.push(append);

  var customInput = ___EmotionJSX(EuiFieldText, _extends({
    value: value,
    fullWidth: true,
    append: appendArray.length ? appendArray : undefined,
    isLoading: status === 'loading' ? true : false,
    onChange: onFieldChange
  }, rest));

  return ___EmotionJSX(EuiInputPopover, {
    className: classes,
    input: customInput,
    isOpen: suggestions.length > 0 && isPopoverOpen,
    panelPaddingSize: "none",
    fullWidth: true,
    closePopover: closePopover
  }, suggestions);
};