/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { EuiI18nConsumer } from '../context';
import { jsx as ___EmotionJSX } from "@emotion/react";
var defaultFormatter = new Intl.NumberFormat('en');

function defaultFormatNumber(value) {
  return defaultFormatter.format(value);
}

function hasValues(x) {
  return x.values != null;
}

var EuiI18nNumber = function EuiI18nNumber(props) {
  return ___EmotionJSX(EuiI18nConsumer, null, function (i18nConfig) {
    var formatNumber = i18nConfig.formatNumber || defaultFormatNumber;

    if (hasValues(props)) {
      return props.children(props.values.map(function (value) {
        return formatNumber(value);
      }));
    }

    var formattedValue = (formatNumber || defaultFormatNumber)(props.value);

    if (props.children) {
      return props.children(formattedValue);
    } else {
      return formattedValue;
    }
  });
};

export { EuiI18nNumber };