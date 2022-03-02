"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.formatDate = exports.dateFormatAliases = void 0;

var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _predicate = require("../predicate");

var _moment = _interopRequireDefault(require("moment"));

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

var calendar = function calendar(value) {
  var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
  var refTime = options.refTime;
  return (0, _moment.default)(value).calendar(refTime, options);
};

var dateFormatAliases = {
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
exports.dateFormatAliases = dateFormatAliases;

function isStringADateFormat(x) {
  return dateFormatAliases.hasOwnProperty(x);
}

function instanceOfFormatDateConfig(x) {
  return (0, _typeof2.default)(x) === 'object' && (x.hasOwnProperty('format') || x.hasOwnProperty('nil') || x.hasOwnProperty('options'));
}

var formatDate = function formatDate(value) {
  var dateFormatKeyOrConfig = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 'dateTime';

  if ((0, _predicate.isString)(dateFormatKeyOrConfig)) {
    if ((0, _predicate.isNil)(value)) {
      return '';
    }

    var dateFormatStrOrFunc = isStringADateFormat(dateFormatKeyOrConfig) ? dateFormatAliases[dateFormatKeyOrConfig] : dateFormatKeyOrConfig;

    if ((0, _predicate.isFunction)(dateFormatStrOrFunc)) {
      return dateFormatStrOrFunc(value, {});
    }

    if ((0, _predicate.isString)(dateFormatStrOrFunc)) {
      return (0, _moment.default)(value).format(dateFormatStrOrFunc);
    }
  }

  if (instanceOfFormatDateConfig(dateFormatKeyOrConfig)) {
    var _dateFormatKeyOrConfi = dateFormatKeyOrConfig.format,
        format = _dateFormatKeyOrConfi === void 0 ? 'dateTime' : _dateFormatKeyOrConfi,
        _dateFormatKeyOrConfi2 = dateFormatKeyOrConfig.nil,
        nil = _dateFormatKeyOrConfi2 === void 0 ? '' : _dateFormatKeyOrConfi2,
        options = dateFormatKeyOrConfig.options;
    var dateFormat = dateFormatAliases[format] || format;

    if ((0, _predicate.isNil)(value)) {
      return nil;
    }

    if ((0, _predicate.isFunction)(dateFormat)) {
      return dateFormat(value, options);
    }

    if ((0, _predicate.isString)(dateFormat)) {
      return (0, _moment.default)(value).format(dateFormat);
    }
  }

  throw new Error("Failed to format value using dateFormatKeyOrConfig: ".concat(dateFormatKeyOrConfig));
};

exports.formatDate = formatDate;