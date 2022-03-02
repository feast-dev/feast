/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { isDateLike, isNumber } from '../../../services/predicate';
import { dateFormat as defaultDateFormat, dateGranularity } from './date_format'; // ESLint doesn't realise that we can import Moment directly.
// eslint-disable-next-line import/named

import moment from 'moment';
export var DATE_TYPE = 'date';
export var dateValuesEqual = function dateValuesEqual(v1, v2) {
  return v1.raw === v2.raw && v1.granularity === v2.granularity && v1.text === v2.text;
};
export var isDateValue = function isDateValue(value) {
  return !!value && value.type === DATE_TYPE && !!value.raw && !!value.text && !!value.resolve;
};
export var dateValue = function dateValue(raw, granularity) {
  var dateFormat = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : defaultDateFormat;

  if (!raw) {
    return undefined;
  }

  if (isDateLike(raw)) {
    var _dateValue = {
      type: DATE_TYPE,
      raw: raw,
      granularity: granularity,
      text: dateFormat.print(raw),
      resolve: function resolve() {
        return moment(raw);
      }
    };
    return _dateValue;
  }

  if (isNumber(raw)) {
    return {
      type: DATE_TYPE,
      raw: raw,
      granularity: granularity,
      text: raw.toString(),
      resolve: function resolve() {
        return moment(raw);
      }
    };
  }

  var text = raw.toString();
  return {
    type: DATE_TYPE,
    raw: raw,
    granularity: granularity,
    text: text,
    resolve: function resolve() {
      return dateFormat.parse(text);
    }
  };
};
export var dateValueParser = function dateValueParser() {
  var format = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : defaultDateFormat;
  return function (text) {
    var parsed = format.parse(text);
    return dateValue(text, dateGranularity(parsed), format);
  };
};