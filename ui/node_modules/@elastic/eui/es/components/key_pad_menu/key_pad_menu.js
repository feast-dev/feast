function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

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
import { EuiFormLabel } from '../form/form_label/form_label';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiKeyPadMenu = function EuiKeyPadMenu(_ref) {
  var children = _ref.children,
      className = _ref.className,
      checkable = _ref.checkable,
      rest = _objectWithoutProperties(_ref, ["children", "className", "checkable"]);

  var classes = classNames('euiKeyPadMenu', className);
  var legend = _typeof(checkable) === 'object' && checkable.legend ? ___EmotionJSX(EuiFormLabel, _extends({}, checkable.legendProps, {
    type: "legend"
  }), checkable.legend) : undefined;
  return checkable ? ___EmotionJSX("fieldset", _extends({
    className: classes,
    "aria-label": _typeof(checkable) === 'object' ? checkable.ariaLegend : undefined
  }, rest), legend, children) : ___EmotionJSX("ul", _extends({
    className: classes
  }, rest), React.Children.map(children, function (child) {
    return ___EmotionJSX("li", null, child);
  }));
};
EuiKeyPadMenu.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Renders the the group as a `fieldset`.
       * Set to `true` to customize the labelling, or pass an #EuiKeyPadMenuCheckableProps object to add a `legend` or `ariaLegend`
       */
  checkable: PropTypes.oneOfType([PropTypes.shape({
    /**
         * Rendered within a `legend` to label the `fieldset`.
         * To create a visually hidden legend, use `ariaLegend`
         */
    legend: PropTypes.node,

    /**
         * Pass through props to a `EuiFormLabel` component, except for `type`
         */
    legendProps: PropTypes.any,

    /**
         * Custom aria-attribute for creating a *visually hidden* legend.
         * To create a visible legend, use `legend`
         */
    ariaLegend: PropTypes.string
  }).isRequired, PropTypes.oneOf([true])])
};