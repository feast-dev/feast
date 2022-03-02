function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { isNil, isFunction, isString } from '../predicate';
import moment from 'moment';

var calendar = function calendar(value) {
  var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
  var refTime = options.refTime;
  return moment(value).calendar(refTime, options);
};

export var dateFormatAliases = {
  date: 'D MMM YYYY',
  longDate: 'DD MMMM YYYY',
  shortDate: 'D MMM YY',
  dateTime: 'D MMM YYYY HH:mm',
  longDateTime: 'DD MMMM YYYY HH:mm:ss',
  shortDateTime: 'D MMM YY HH:mm',
  dobShort: 'Do MMM YY',
  dobLong: 'Do MMMM YYYY',
  iso8601: 'YYYY-MM-DDTHH:mm:ss.SSSZ',
  calendar: calendar,
  calendarDateTime: function calendarDateTime(value, options) {
    return calendar(value, _objectSpread({
      sameDay: '[Today at] H:mmA',
      nextDay: '[Tomorrow at] H:mmA',
      nextWeek: 'dddd [at] H:mmA',
      lastDay: '[Yesterday at] H:mmA',
      lastWeek: '[Last] dddd [at] H:mmA',
      sameElse: 'Do MMM YYYY [at] H:mmA'
    }, options));
  },
  calendarDate: function calendarDate(value, options) {
    return calendar(value, _objectSpread({
      sameDay: '[Today]',
      nextDay: '[Tomorrow]',
      nextWeek: 'dddd',
      lastDay: '[Yesterday]',
      lastWeek: '[Last] dddd',
      sameElse: 'Do MMM YYYY'
    }, options));
  }
};

function isStringADateFormat(x) {
  return dateFormatAliases.hasOwnProperty(x);
}

function instanceOfFormatDateConfig(x) {
  return _typeof(x) === 'object' && (x.hasOwnProperty('format') || x.hasOwnProperty('nil') || x.hasOwnProperty('options'));
}

export var formatDate = function formatDate(value) {
  var dateFormatKeyOrConfig = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 'dateTime';

  if (isString(dateFormatKeyOrConfig)) {
    if (isNil(value)) {
      return '';
    }

    var dateFormatStrOrFunc = isStringADateFormat(dateFormatKeyOrConfig) ? dateFormatAliases[dateFormatKeyOrConfig] : dateFormatKeyOrConfig;

    if (isFunction(dateFormatStrOrFunc)) {
      return dateFormatStrOrFunc(value, {});
    }

    if (isString(dateFormatStrOrFunc)) {
      return moment(value).format(dateFormatStrOrFunc);
    }
  }

  if (instanceOfFormatDateConfig(dateFormatKeyOrConfig)) {
    var _dateFormatKeyOrConfi = dateFormatKeyOrConfig.format,
        format = _dateFormatKeyOrConfi === void 0 ? 'dateTime' : _dateFormatKeyOrConfi,
        _dateFormatKeyOrConfi2 = dateFormatKeyOrConfig.nil,
        nil = _dateFormatKeyOrConfi2 === void 0 ? '' : _dateFormatKeyOrConfi2,
        options = dateFormatKeyOrConfig.options;
    var dateFormat = dateFormatAliases[format] || format;

    if (isNil(value)) {
      return nil;
    }

    if (isFunction(dateFormat)) {
      return dateFormat(value, options);
    }

    if (isString(dateFormat)) {
      return moment(value).format(dateFormat);
    }
  }

  throw new Error("Failed to format value using dateFormatKeyOrConfig: ".concat(dateFormatKeyOrConfig));
};