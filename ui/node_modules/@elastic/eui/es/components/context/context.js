/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { createContext } from 'react';
import PropTypes from "prop-types";
import { jsx as ___EmotionJSX } from "@emotion/react";
var I18nContext = /*#__PURE__*/createContext({});
var EuiI18nProvider = I18nContext.Provider,
    EuiI18nConsumer = I18nContext.Consumer;

var EuiContext = function EuiContext(_ref) {
  var _ref$i18n = _ref.i18n,
      i18n = _ref$i18n === void 0 ? {} : _ref$i18n,
      children = _ref.children;
  return ___EmotionJSX(EuiI18nProvider, {
    value: i18n
  }, children);
};

EuiContext.propTypes = {
  i18n: PropTypes.shape({
    mapping: PropTypes.shape({}),
    mappingFunc: PropTypes.func,

    /**
       * Some browsers' translation features don't work with a rendered `<Fragment>` component.
       * The `render` function allows you to pass in another component instead, e.g. `<div>`
       */
    render: PropTypes.func,
    formatNumber: PropTypes.func,
    formatDateTime: PropTypes.func,
    locale: PropTypes.string
  }).isRequired,

  /**
     * ReactNode to render as this component's content
     */
  children: PropTypes.node.isRequired
};
export { EuiContext, EuiI18nConsumer, I18nContext };