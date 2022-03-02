import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { forwardRef } from 'react';
import classNames from 'classnames';
import { EuiIcon } from '../icon';
import { EuiI18n, useEuiI18n } from '../i18n';
import { keysOf } from '../common';
import { getSecureRelForTarget } from '../../services';
import { EuiScreenReaderOnly } from '../accessibility';
import { validateHref } from '../../services/security/href_validator';
import { jsx as ___EmotionJSX } from "@emotion/react";
var colorsToClassNameMap = {
  primary: 'euiLink--primary',
  subdued: 'euiLink--subdued',
  success: 'euiLink--success',
  accent: 'euiLink--accent',
  danger: 'euiLink--danger',
  warning: 'euiLink--warning',
  ghost: 'euiLink--ghost',
  text: 'euiLink--text'
};
export var COLORS = keysOf(colorsToClassNameMap);
var EuiLink = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var children = _ref.children,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'primary' : _ref$color,
      className = _ref.className,
      href = _ref.href,
      external = _ref.external,
      target = _ref.target,
      rel = _ref.rel,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'button' : _ref$type,
      onClick = _ref.onClick,
      _disabled = _ref.disabled,
      rest = _objectWithoutProperties(_ref, ["children", "color", "className", "href", "external", "target", "rel", "type", "onClick", "disabled"]);

  var isHrefValid = !href || validateHref(href);
  var disabled = _disabled || !isHrefValid;

  var externalLinkIcon = ___EmotionJSX(EuiIcon, {
    "aria-label": useEuiI18n('euiLink.external.ariaLabel', 'External link'),
    size: "s",
    className: "euiLink__externalIcon",
    type: "popout"
  });

  var newTargetScreenreaderText = ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, ___EmotionJSX(EuiI18n, {
    token: "euiLink.newTarget.screenReaderOnlyText",
    default: "(opens in a new tab or window)"
  })));

  if (href === undefined || !isHrefValid) {
    var buttonProps = _objectSpread({
      className: classNames('euiLink', disabled ? 'euiLink-disabled' : colorsToClassNameMap[color], className),
      type: type,
      onClick: onClick,
      disabled: disabled
    }, rest);

    return ___EmotionJSX("button", _extends({
      ref: ref
    }, buttonProps), children);
  }

  var secureRel = getSecureRelForTarget({
    href: href,
    target: target,
    rel: rel
  });

  var anchorProps = _objectSpread({
    className: classNames('euiLink', colorsToClassNameMap[color], className),
    href: href,
    target: target,
    rel: secureRel,
    onClick: onClick
  }, rest);

  var showExternalLinkIcon = target === '_blank' && external !== false || external === true;
  return ___EmotionJSX("a", _extends({
    ref: ref
  }, anchorProps), children, showExternalLinkIcon && externalLinkIcon, target === '_blank' && newTargetScreenreaderText);
});
EuiLink.displayName = 'EuiLink';
export { EuiLink };