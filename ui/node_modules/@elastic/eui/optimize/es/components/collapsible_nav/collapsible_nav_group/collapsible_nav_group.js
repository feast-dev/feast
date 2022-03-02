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
import { useGeneratedHtmlId } from '../../../services';
import { EuiAccordion } from '../../accordion';
import { EuiIcon } from '../../icon';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { EuiTitle } from '../../title';
import { jsx as ___EmotionJSX } from "@emotion/react";
var backgroundToClassNameMap = {
  none: '',
  light: 'euiCollapsibleNavGroup--light',
  dark: 'euiCollapsibleNavGroup--dark'
};
export var BACKGROUNDS = Object.keys(backgroundToClassNameMap);
export var EuiCollapsibleNavGroup = function EuiCollapsibleNavGroup(_ref) {
  var className = _ref.className,
      children = _ref.children,
      id = _ref.id,
      title = _ref.title,
      iconType = _ref.iconType,
      _ref$iconSize = _ref.iconSize,
      iconSize = _ref$iconSize === void 0 ? 'l' : _ref$iconSize,
      _ref$background = _ref.background,
      background = _ref$background === void 0 ? 'none' : _ref$background,
      _ref$isCollapsible = _ref.isCollapsible,
      isCollapsible = _ref$isCollapsible === void 0 ? false : _ref$isCollapsible,
      _ref$titleElement = _ref.titleElement,
      titleElement = _ref$titleElement === void 0 ? 'h3' : _ref$titleElement,
      _ref$titleSize = _ref.titleSize,
      titleSize = _ref$titleSize === void 0 ? 'xxs' : _ref$titleSize,
      iconProps = _ref.iconProps,
      rest = _objectWithoutProperties(_ref, ["className", "children", "id", "title", "iconType", "iconSize", "background", "isCollapsible", "titleElement", "titleSize", "iconProps"]);

  var groupID = useGeneratedHtmlId({
    conditionalId: id
  });
  var titleID = "".concat(groupID, "__title");
  var classes = classNames('euiCollapsibleNavGroup', backgroundToClassNameMap[background], {
    'euiCollapsibleNavGroup--withHeading': title
  }, className); // Warn if consumer passes an iconType without a title

  if (iconType && !title) {
    console.warn('EuiCollapsibleNavGroup will not render an icon without `title`.');
  }

  var content = children && ___EmotionJSX("div", {
    className: "euiCollapsibleNavGroup__children"
  }, children);

  var headingClasses = 'euiCollapsibleNavGroup__heading';
  var TitleElement = titleElement;
  var titleContent = title ? ___EmotionJSX(EuiFlexGroup, {
    gutterSize: "m",
    alignItems: "center",
    responsive: false
  }, iconType && ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, ___EmotionJSX(EuiIcon, _extends({}, iconProps, {
    type: iconType,
    size: iconSize
  }))), ___EmotionJSX(EuiFlexItem, null, ___EmotionJSX(EuiTitle, {
    size: titleSize
  }, ___EmotionJSX(TitleElement, {
    id: titleID,
    className: "euiCollapsibleNavGroup__title"
  }, title)))) : undefined;

  if (isCollapsible && title) {
    return ___EmotionJSX(EuiAccordion, _extends({
      id: groupID,
      className: classes,
      buttonClassName: headingClasses,
      buttonContent: titleContent,
      initialIsOpen: true,
      arrowDisplay: "right",
      arrowProps: {
        color: background === 'dark' ? 'ghost' : 'text'
      }
    }, rest), content);
  } else {
    return ___EmotionJSX("div", _extends({
      id: groupID,
      className: classes
    }, rest), titleContent && ___EmotionJSX("div", {
      className: headingClasses
    }, titleContent), content);
  }
};