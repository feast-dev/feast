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
import { EuiBreadcrumbs } from '../../breadcrumbs';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiHeaderBreadcrumbs = function EuiHeaderBreadcrumbs(_ref) {
  var className = _ref.className,
      breadcrumbs = _ref.breadcrumbs,
      rest = _objectWithoutProperties(_ref, ["className", "breadcrumbs"]);

  var classes = classNames('euiHeaderBreadcrumbs', className);
  return ___EmotionJSX(EuiBreadcrumbs, _extends({
    max: 4,
    truncate: true,
    breadcrumbs: breadcrumbs,
    className: classes
  }, rest));
};
EuiHeaderBreadcrumbs.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Hides extra (above the max) breadcrumbs under a collapsed item as the window gets smaller.
     * Pass a custom #EuiBreadcrumbResponsiveMaxCount object to change the number of breadcrumbs to show at the particular breakpoints.
     *
     * Pass `false` to turn this behavior off.
     *
     * Default: `{ xs: 1, s: 2, m: 4 }`
     */
  responsive: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.any.isRequired]),

  /**
     * Forces all breadcrumbs to single line and
     * truncates each breadcrumb to a particular width,
     * except for the last item
     */
  truncate: PropTypes.bool,

  /**
     * Collapses the inner items past the maximum set here
     * into a single ellipses item.
     * Omitting or passing a `0` value will show all breadcrumbs.
     */
  max: PropTypes.oneOfType([PropTypes.number.isRequired, PropTypes.oneOf([null])]),

  /**
     * The array of individual #EuiBreadcrumb items
     */
  breadcrumbs: PropTypes.arrayOf(PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,

    /**
       * Visible label of the breadcrumb
       */
    text: PropTypes.node.isRequired,
    href: PropTypes.string,
    onClick: PropTypes.func,

    /**
       * Force a max-width on the breadcrumb text
       */
    truncate: PropTypes.bool,

    /**
       * Override the existing `aria-current` which defaults to `page` for the last breadcrumb
       */
    "aria-current": PropTypes.any
  }).isRequired).isRequired
};