/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { isString } from '../../services/predicate';
import { EuiButtonEmpty, EuiButtonIcon } from '../button';
import { EuiToolTip } from '../tool_tip';
import { useGeneratedHtmlId } from '../../services/accessibility';
import { EuiScreenReaderOnly } from '../accessibility';
import { jsx as ___EmotionJSX } from "@emotion/react";
// In order to use generics with an arrow function inside a .tsx file, it's necessary to use
// this `extends` hack and declare the types as shown, instead of declaring the const as a
// FunctionComponent
export var DefaultItemAction = function DefaultItemAction(_ref) {
  var action = _ref.action,
      enabled = _ref.enabled,
      item = _ref.item,
      className = _ref.className;

  if (!action.onClick && !action.href) {
    throw new Error("Cannot render item action [".concat(action.name, "]. Missing required 'onClick' callback\n      or 'href' string. If you want to provide a custom action control, make sure to define the 'render' callback"));
  }

  var onClick = action.onClick ? function () {
    return action.onClick(item);
  } : undefined;
  var buttonColor = action.color;
  var color = 'primary';

  if (buttonColor) {
    color = isString(buttonColor) ? buttonColor : buttonColor(item);
  }

  var buttonIcon = action.icon;
  var icon;

  if (buttonIcon) {
    icon = isString(buttonIcon) ? buttonIcon : buttonIcon(item);
  }

  var button;
  var actionContent = typeof action.name === 'function' ? action.name(item) : action.name;
  var ariaLabelId = useGeneratedHtmlId();

  if (action.type === 'icon') {
    if (!icon) {
      throw new Error("Cannot render item action [".concat(action.name, "]. It is configured to render as an icon but no\n      icon is provided. Make sure to set the 'icon' property of the action"));
    }

    button = ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiButtonIcon, {
      className: className,
      "aria-labelledby": ariaLabelId,
      isDisabled: !enabled,
      color: color,
      iconType: icon,
      onClick: onClick,
      href: action.href,
      target: action.target,
      "data-test-subj": action['data-test-subj']
    }), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", {
      id: ariaLabelId
    }, actionContent)));
  } else {
    button = ___EmotionJSX(EuiButtonEmpty, {
      className: className,
      size: "s",
      isDisabled: !enabled,
      color: color,
      iconType: icon,
      onClick: onClick,
      href: action.href,
      target: action.target,
      "data-test-subj": action['data-test-subj'],
      flush: "right"
    }, actionContent);
  }

  return enabled && action.description ? ___EmotionJSX(EuiToolTip, {
    content: action.description,
    delay: "long"
  }, button) : button;
};