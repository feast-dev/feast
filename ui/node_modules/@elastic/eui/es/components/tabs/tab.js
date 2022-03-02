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
import { getSecureRelForTarget } from '../../services';
import { validateHref } from '../../services/security/href_validator';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiTab = function EuiTab(_ref) {
  var isSelected = _ref.isSelected,
      children = _ref.children,
      className = _ref.className,
      _disabled = _ref.disabled,
      href = _ref.href,
      target = _ref.target,
      rel = _ref.rel,
      prepend = _ref.prepend,
      append = _ref.append,
      rest = _objectWithoutProperties(_ref, ["isSelected", "children", "className", "disabled", "href", "target", "rel", "prepend", "append"]);

  var isHrefValid = !href || validateHref(href);
  var disabled = _disabled || !isHrefValid;
  var classes = classNames('euiTab', className, {
    'euiTab-isSelected': isSelected,
    'euiTab-isDisabled': disabled
  });

  var prependNode = prepend && ___EmotionJSX("span", {
    className: "euiTab__prepend"
  }, prepend);

  var appendNode = append && ___EmotionJSX("span", {
    className: "euiTab__append"
  }, append); //  <a> elements don't respect the `disabled` attribute. So if we're disabled, we'll just pretend
  //  this is a button and piggyback off its disabled styles.


  if (href && !disabled) {
    var secureRel = getSecureRelForTarget({
      href: href,
      target: target,
      rel: rel
    });
    return ___EmotionJSX("a", _extends({
      role: "tab",
      "aria-selected": !!isSelected,
      className: classes,
      href: href,
      target: target,
      rel: secureRel
    }, rest), prependNode, ___EmotionJSX("span", {
      className: "euiTab__content"
    }, children), appendNode);
  }

  return ___EmotionJSX("button", _extends({
    role: "tab",
    "aria-selected": !!isSelected,
    type: "button",
    className: classes,
    disabled: disabled
  }, rest), prependNode, ___EmotionJSX("span", {
    className: "euiTab__content"
  }, children), appendNode);
};
EuiTab.propTypes = {
  isSelected: PropTypes.bool,
  disabled: PropTypes.bool,

  /**
     * Places content before the tab content/children.
     * Will be excluded from interactive effects.
     */

  /**
     * Places content before the tab content/children.
     * Will be excluded from interactive effects.
     */
  prepend: PropTypes.node,

  /**
     * Places content after the tab content/children.
     * Will be excluded from interactive effects.
     */

  /**
     * Places content after the tab content/children.
     * Will be excluded from interactive effects.
     */
  append: PropTypes.node,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  href: PropTypes.string,
  onClick: PropTypes.func
};