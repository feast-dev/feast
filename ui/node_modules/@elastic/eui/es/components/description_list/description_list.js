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
import { EuiDescriptionListTitle } from './description_list_title';
import { EuiDescriptionListDescription } from './description_list_description';
import { keysOf } from '../common';
import { jsx as ___EmotionJSX } from "@emotion/react";
var typesToClassNameMap = {
  row: 'euiDescriptionList--row',
  inline: 'euiDescriptionList--inline',
  column: 'euiDescriptionList--column',
  responsiveColumn: 'euiDescriptionList--responsiveColumn'
};
export var TYPES = keysOf(typesToClassNameMap);
var alignmentsToClassNameMap = {
  center: 'euiDescriptionList--center',
  left: ''
};
export var ALIGNMENTS = keysOf(alignmentsToClassNameMap);
var textStylesToClassNameMap = {
  normal: '',
  reverse: 'euiDescriptionList--reverse'
};
export var TEXT_STYLES = keysOf(textStylesToClassNameMap);
export var EuiDescriptionList = function EuiDescriptionList(_ref) {
  var _ref$align = _ref.align,
      align = _ref$align === void 0 ? 'left' : _ref$align,
      children = _ref.children,
      className = _ref.className,
      _ref$compressed = _ref.compressed,
      compressed = _ref$compressed === void 0 ? false : _ref$compressed,
      descriptionProps = _ref.descriptionProps,
      listItems = _ref.listItems,
      _ref$textStyle = _ref.textStyle,
      textStyle = _ref$textStyle === void 0 ? 'normal' : _ref$textStyle,
      titleProps = _ref.titleProps,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'row' : _ref$type,
      rest = _objectWithoutProperties(_ref, ["align", "children", "className", "compressed", "descriptionProps", "listItems", "textStyle", "titleProps", "type"]);

  var classes = classNames('euiDescriptionList', type ? typesToClassNameMap[type] : undefined, align ? alignmentsToClassNameMap[align] : undefined, textStyle ? textStylesToClassNameMap[textStyle] : undefined, {
    'euiDescriptionList--compressed': compressed
  }, className);
  var childrenOrListItems = null;

  if (listItems) {
    childrenOrListItems = listItems.map(function (item, index) {
      return [___EmotionJSX(EuiDescriptionListTitle, _extends({
        key: "title-".concat(index)
      }, titleProps), item.title), ___EmotionJSX(EuiDescriptionListDescription, _extends({
        key: "description-".concat(index)
      }, descriptionProps), item.description)];
    });
  } else {
    childrenOrListItems = children;
  }

  return ___EmotionJSX("dl", _extends({
    className: classes
  }, rest), childrenOrListItems);
};
EuiDescriptionList.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  listItems: PropTypes.arrayOf(PropTypes.shape({
    title: PropTypes.any.isRequired,
    description: PropTypes.any.isRequired
  }).isRequired),

  /**
     * Text alignment
     */
  align: PropTypes.oneOf(["center", "left"]),

  /**
     * Smaller text and condensed spacing
     */
  compressed: PropTypes.bool,

  /**
     * How should the content be styled, by default
     * this will emphasize the title
     */
  textStyle: PropTypes.oneOf(["normal", "reverse"]),

  /**
     * How each item should be laid out
     */
  type: PropTypes.oneOf(["row", "inline", "column", "responsiveColumn"]),

  /**
     * Props object to be passed to `EuiDescriptionListTitle`
     */
  titleProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }),

  /**
     * Props object to be passed to `EuiDescriptionListDescription`
     */
  descriptionProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  })
};