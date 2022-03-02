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
EuiDescribedFormGroup.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * One or more `EuiFormRow`s
       */
  children: PropTypes.node,

  /**
       * Passed to `EuiFlexGroup`
       */
  gutterSize: PropTypes.oneOf(["none", "xs", "s", "m", "l", "xl"]),
  fullWidth: PropTypes.bool,

  /**
       * For better accessibility, it's recommended the use of HTML headings
       */
  title: PropTypes.element.isRequired,
  titleSize: PropTypes.oneOf(["xxxs", "xxs", "xs", "s", "m", "l"]),

  /**
       * Added as a child of `EuiText`
       */
  description: PropTypes.node,

  /**
       * For customizing the field container. Extended from `EuiFlexItem`
       */
  descriptionFlexItemProps: PropTypes.any,
  fieldFlexItemProps: PropTypes.any
};