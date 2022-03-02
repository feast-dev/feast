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
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiFlyoutBody = function EuiFlyoutBody(_ref) {
  var children = _ref.children,
      className = _ref.className,
      banner = _ref.banner,
      rest = _objectWithoutProperties(_ref, ["children", "className", "banner"]);

  var classes = classNames('euiFlyoutBody', className);
  var overflowClasses = classNames('euiFlyoutBody__overflow', {
    'euiFlyoutBody__overflow--hasBanner': banner
  });
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), ___EmotionJSX("div", {
    tabIndex: 0,
    className: overflowClasses
  }, banner && ___EmotionJSX("div", {
    className: "euiFlyoutBody__banner"
  }, banner), ___EmotionJSX("div", {
    className: "euiFlyoutBody__overflowContent"
  }, children)));
};
EuiFlyoutBody.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
         * Use to display a banner at the top of the body. It is suggested to use `EuiCallOut` for it.
         */
  banner: PropTypes.node
};