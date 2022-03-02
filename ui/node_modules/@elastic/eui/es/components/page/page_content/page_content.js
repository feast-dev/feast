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
import { EuiPanel } from '../../panel/panel';
import { jsx as ___EmotionJSX } from "@emotion/react";
var verticalPositionToClassNameMap = {
  center: 'euiPageContent--verticalCenter'
};
var horizontalPositionToClassNameMap = {
  center: 'euiPageContent--horizontalCenter'
};
export var EuiPageContent = function EuiPageContent(_ref) {
  var verticalPosition = _ref.verticalPosition,
      horizontalPosition = _ref.horizontalPosition,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'l' : _ref$paddingSize,
      borderRadius = _ref.borderRadius,
      children = _ref.children,
      className = _ref.className,
      _ref$role = _ref.role,
      _role = _ref$role === void 0 ? 'main' : _ref$role,
      rest = _objectWithoutProperties(_ref, ["verticalPosition", "horizontalPosition", "paddingSize", "borderRadius", "children", "className", "role"]);

  var role = _role === null ? undefined : _role;
  var borderRadiusClass = borderRadius === 'none' ? 'euiPageContent--borderRadiusNone' : '';
  var classes = classNames('euiPageContent', borderRadiusClass, verticalPosition ? verticalPositionToClassNameMap[verticalPosition] : null, horizontalPosition ? horizontalPositionToClassNameMap[horizontalPosition] : null, className);
  return ___EmotionJSX(EuiPanel, _extends({
    className: classes,
    paddingSize: paddingSize,
    borderRadius: borderRadius,
    role: role
  }, rest), children);
};
EuiPageContent.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Adds a medium shadow to the panel;
     * Only works when `color="plain"`
     */

  /**
     * Adds a medium shadow to the panel;
     * Only works when `color="plain"`
     */
  hasShadow: PropTypes.bool,

  /**
     * Adds a slight 1px border on all edges.
     * Only works when `color="plain | transparent"`
     * Default is `undefined` and will default to that theme's panel style
     */

  /**
     * Adds a slight 1px border on all edges.
     * Only works when `color="plain | transparent"`
     * Default is `undefined` and will default to that theme's panel style
     */
  hasBorder: PropTypes.bool,

  /**
     * Padding for all four sides
     */

  /**
     * Padding for all four sides
     */
  paddingSize: PropTypes.any,

  /**
     * Corner border radius
     */

  /**
     * Corner border radius
     */
  borderRadius: PropTypes.any,

  /**
     * When true the panel will grow in height to match `EuiFlexItem`
     */

  /**
     * When true the panel will grow in height to match `EuiFlexItem`
     */
  grow: PropTypes.bool,
  panelRef: PropTypes.any,

  /**
     * Background color of the panel;
     * Usually a lightened form of the brand colors
     */

  /**
     * Background color of the panel;
     * Usually a lightened form of the brand colors
     */
  color: PropTypes.any,
  element: PropTypes.oneOf(["div"]),
  verticalPosition: PropTypes.oneOf(["center"]),
  horizontalPosition: PropTypes.oneOf(["center"]),

  /**
       * There should only be one EuiPageContent per page and should contain the main contents.
       * If this is untrue, set role = `null`, or change it to match your needed aria role
       */
  role: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.oneOf([null])])
};