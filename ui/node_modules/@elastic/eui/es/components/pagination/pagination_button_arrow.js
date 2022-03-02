function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

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
import { EuiButtonIcon } from '../button/button_icon';
import { keysOf } from '../common';
import { useEuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
var typeToIconTypeMap = {
  first: 'arrowStart',
  previous: 'arrowLeft',
  next: 'arrowRight',
  last: 'arrowEnd'
};
export var TYPES = keysOf(typeToIconTypeMap);
export var EuiPaginationButtonArrow = function EuiPaginationButtonArrow(_ref) {
  var className = _ref.className,
      type = _ref.type,
      disabled = _ref.disabled,
      ariaControls = _ref.ariaControls,
      onClick = _ref.onClick;
  var labels = {
    first: useEuiI18n('euiPaginationButtonArrow.firstPage', 'First page'),
    previous: useEuiI18n('euiPaginationButtonArrow.previousPage', 'Previous page'),
    next: useEuiI18n('euiPaginationButtonArrow.nextPage', 'Next page'),
    last: useEuiI18n('euiPaginationButtonArrow.lastPage', 'Last page')
  };
  var buttonProps = {};

  if (ariaControls && !disabled) {
    buttonProps.href = "#".concat(ariaControls);
    buttonProps['aria-controls'] = ariaControls;
  }

  return ___EmotionJSX(EuiButtonIcon, _extends({
    className: classNames('euiPaginationArrowButton', className),
    color: "text",
    "aria-label": labels[type],
    title: disabled ? undefined : labels[type],
    isDisabled: disabled,
    onClick: onClick,
    "data-test-subj": "pagination-button-".concat(type),
    iconType: typeToIconTypeMap[type]
  }, buttonProps));
};
EuiPaginationButtonArrow.propTypes = {
  type: PropTypes.any.isRequired,
  disabled: PropTypes.bool,
  ariaControls: PropTypes.string
};