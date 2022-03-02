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
import { keysOf } from '../../common';
import { EuiTitle } from '../../title';
import { EuiText } from '../../text';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { jsx as ___EmotionJSX } from "@emotion/react";
var paddingSizeToClassNameMap = {
  xxxs: 'euiDescribedFormGroup__fieldPadding--xxxsmall',
  xxs: 'euiDescribedFormGroup__fieldPadding--xxsmall',
  xs: 'euiDescribedFormGroup__fieldPadding--xsmall',
  s: 'euiDescribedFormGroup__fieldPadding--small',
  m: 'euiDescribedFormGroup__fieldPadding--medium',
  l: 'euiDescribedFormGroup__fieldPadding--large'
};
export var PADDING_SIZES = keysOf(paddingSizeToClassNameMap);
export var EuiDescribedFormGroup = function EuiDescribedFormGroup(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$gutterSize = _ref.gutterSize,
      gutterSize = _ref$gutterSize === void 0 ? 'l' : _ref$gutterSize,
      _ref$fullWidth = _ref.fullWidth,
      fullWidth = _ref$fullWidth === void 0 ? false : _ref$fullWidth,
      _ref$titleSize = _ref.titleSize,
      titleSize = _ref$titleSize === void 0 ? 'xs' : _ref$titleSize,
      title = _ref.title,
      description = _ref.description,
      descriptionFlexItemProps = _ref.descriptionFlexItemProps,
      fieldFlexItemProps = _ref.fieldFlexItemProps,
      rest = _objectWithoutProperties(_ref, ["children", "className", "gutterSize", "fullWidth", "titleSize", "title", "description", "descriptionFlexItemProps", "fieldFlexItemProps"]);

  var classes = classNames('euiDescribedFormGroup', {
    'euiDescribedFormGroup--fullWidth': fullWidth
  }, className);
  var fieldClasses = classNames('euiDescribedFormGroup__fields', paddingSizeToClassNameMap[titleSize], fieldFlexItemProps && fieldFlexItemProps.className);
  var renderedDescription;

  if (description) {
    renderedDescription = ___EmotionJSX(EuiText, {
      size: "s",
      color: "subdued",
      className: "euiDescribedFormGroup__description"
    }, description);
  }

  return ___EmotionJSX("div", _extends({
    role: "group",
    className: classes
  }, rest), ___EmotionJSX(EuiFlexGroup, {
    gutterSize: gutterSize
  }, ___EmotionJSX(EuiFlexItem, descriptionFlexItemProps, ___EmotionJSX(EuiTitle, {
    size: titleSize,
    className: "euiDescribedFormGroup__title"
  }, title), renderedDescription), ___EmotionJSX(EuiFlexItem, _extends({}, fieldFlexItemProps, {
    className: fieldClasses
  }), children)));
};