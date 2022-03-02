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
import { EuiIcon } from '../icon';
import { getSecureRelForTarget } from '../../services';
import { validateHref } from '../../services/security/href_validator';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiHeaderLogo = function EuiHeaderLogo(_ref) {
  var _ref$iconType = _ref.iconType,
      iconType = _ref$iconType === void 0 ? 'logoElastic' : _ref$iconType,
      _ref$iconTitle = _ref.iconTitle,
      iconTitle = _ref$iconTitle === void 0 ? 'Elastic' : _ref$iconTitle,
      href = _ref.href,
      rel = _ref.rel,
      target = _ref.target,
      children = _ref.children,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["iconType", "iconTitle", "href", "rel", "target", "children", "className"]);

  var classes = classNames('euiHeaderLogo', className);
  var secureRel = getSecureRelForTarget({
    href: href,
    rel: rel,
    target: target
  });
  var isHrefValid = !href || validateHref(href);
  return ___EmotionJSX("a", _extends({
    href: isHrefValid ? href : '',
    rel: secureRel,
    target: target,
    className: classes
  }, rest), ___EmotionJSX(EuiIcon, {
    "aria-label": iconTitle,
    className: "euiHeaderLogo__icon",
    size: "l",
    type: iconType
  }), children && ___EmotionJSX("span", {
    className: "euiHeaderLogo__text"
  }, children));
};