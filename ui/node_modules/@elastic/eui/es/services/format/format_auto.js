/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { isNil, isArray, isBoolean, isDate, isNaN, isNumber, isString } from '../predicate';
import { formatBoolean } from './format_boolean';
import { formatDate } from './format_date';
import { formatNumber } from './format_number';
import { formatText } from './format_text';
export var formatAuto = function formatAuto(value) {
  if (isNil(value) || isNaN(value)) {
    return '';
  }

  if (isString(value)) {
    return formatText(value);
  }

  if (isDate(value)) {
    return formatDate(value);
  }

  if (isBoolean(value)) {
    return formatBoolean(value);
  }

  if (isNumber(value)) {
    return formatNumber(value);
  }

  if (isArray(value)) {
    return Array.isArray(value) ? value.map(function (item) {
      return formatAuto(item);
    }).join(', ') : formatAuto(value);
  } // TODO not sure if we want that.. the (+) is that we show something, the (-) is that it's very technical


  return JSON.stringify(value);
};