"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.lte = exports.lt = exports.gte = exports.gt = exports.exact = exports.eq = void 0;

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _date_format = require("./date_format");

var _date_value = require("./date_value");

var _predicate = require("../../../services/predicate");

var _moment = _interopRequireDefault(require("moment"));

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

var utc = _moment.default.utc;

var resolveValueAsDate = function resolveValueAsDate(value) {
  if (_moment.default.isMoment(value)) {
    return value;
  }

  if (_moment.default.isDate(value) || (0, _predicate.isNumber)(value)) {
    return (0, _moment.default)(value);
  }

  return _date_format.dateFormat.parse(String(value));
};

var defaultEqOptions = {
  ignoreCase: true
};

var eq = function eq(fieldValue, clauseValue) {
  var options = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {};
  options = _objectSpread(_objectSpread({}, defaultEqOptions), options);

  if ((0, _predicate.isNil)(fieldValue) || (0, _predicate.isNil)(clauseValue)) {
    return fieldValue === clauseValue;
  }

  if ((0, _predicate.isBoolean)(fieldValue)) {
    return clauseValue === fieldValue;
  }

  if ((0, _predicate.isArray)(fieldValue)) {
    if (fieldValue.length > 0) {
      return fieldValue.some(function (item) {
        return eq(item, clauseValue, options);
      });
    } else {
      return eq('', clauseValue, options);
    }
  }

  if ((0, _date_value.isDateValue)(clauseValue)) {
    var dateFieldValue = resolveValueAsDate(fieldValue);

    if (clauseValue.granularity) {
      return clauseValue.granularity.isSame(dateFieldValue, clauseValue.resolve());
    }

    return dateFieldValue.isSame(clauseValue.resolve());
  }

  if ((0, _predicate.isString)(fieldValue)) {
    if (options.exactMatch === true) {
      return options.ignoreCase ? fieldValue.toLowerCase() === clauseValue.toString().toLowerCase() : fieldValue === clauseValue.toString();
    } else {
      return options.ignoreCase ? fieldValue.toLowerCase().includes(clauseValue.toString().toLowerCase()) : fieldValue.includes(clauseValue.toString());
    }
  }

  if ((0, _predicate.isNumber)(fieldValue)) {
    clauseValue = Number(clauseValue);
    return fieldValue === clauseValue;
  }

  if ((0, _predicate.isDateLike)(fieldValue)) {
    var date = resolveValueAsDate(clauseValue);

    if (!date.isValid()) {
      return false;
    }

    var granularity = (0, _date_format.dateGranularity)(date);

    if (!granularity) {
      return utc(fieldValue).isSame(date);
    }

    return granularity.isSame(fieldValue, date);
  }

  return false; // unknown value type
};

exports.eq = eq;

var exact = function exact(fieldValue, clauseValue) {
  var options = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {};
  return eq(fieldValue, clauseValue, _objectSpread(_objectSpread({}, options), {}, {
    exactMatch: true
  }));
};

exports.exact = exact;

var greaterThen = function greaterThen(fieldValue, clauseValue) {
  var inclusive = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;

  if ((0, _date_value.isDateValue)(clauseValue)) {
    var clauseDateValue = clauseValue.resolve();
    var fieldValueAsMomentInput = fieldValue;

    if (!clauseValue.granularity) {
      return inclusive ? utc(fieldValueAsMomentInput).isSameOrAfter(clauseDateValue) : utc(fieldValueAsMomentInput).isAfter(clauseDateValue);
    }

    if (inclusive) {
      return utc(fieldValueAsMomentInput).isSameOrAfter(clauseValue.granularity.start(clauseDateValue));
    }

    return utc(fieldValueAsMomentInput).isSameOrAfter(clauseValue.granularity.startOfNext(clauseDateValue));
  }

  if ((0, _predicate.isString)(fieldValue)) {
    var str = String(clauseValue);
    return inclusive ? fieldValue >= str : fieldValue > str;
  }

  if ((0, _predicate.isNumber)(fieldValue)) {
    var number = Number(clauseValue);
    return inclusive ? fieldValue >= number : fieldValue > number;
  }

  if ((0, _predicate.isDateLike)(fieldValue)) {
    var date = resolveValueAsDate(clauseValue);
    var granularity = (0, _date_format.dateGranularity)(date);

    if (!granularity) {
      return inclusive ? utc(fieldValue).isSameOrAfter(date) : utc(fieldValue).isAfter(date);
    }

    if (inclusive) {
      return utc(fieldValue).isSameOrAfter(granularity.start(date));
    }

    return utc(fieldValue).isSameOrAfter(granularity.startOfNext(date));
  }

  if ((0, _predicate.isArray)(fieldValue)) {
    return fieldValue.every(function (item) {
      return greaterThen(item, clauseValue, inclusive);
    });
  }

  return false; // unsupported value type
};

var gt = function gt(fieldValue, clauseValue) {
  if ((0, _predicate.isNil)(fieldValue) || (0, _predicate.isNil)(clauseValue)) {
    return false;
  }

  return greaterThen(fieldValue, clauseValue);
};

exports.gt = gt;

var gte = function gte(fieldValue, clauseValue) {
  if ((0, _predicate.isNil)(fieldValue) || (0, _predicate.isNil)(clauseValue)) {
    return fieldValue === clauseValue;
  }

  return greaterThen(fieldValue, clauseValue, true);
};

exports.gte = gte;

var lt = function lt(fieldValue, clauseValue) {
  if ((0, _predicate.isNil)(fieldValue) || (0, _predicate.isNil)(clauseValue)) {
    return false;
  }

  return !greaterThen(fieldValue, clauseValue, true);
};

exports.lt = lt;

var lte = function lte(fieldValue, clauseValue) {
  if ((0, _predicate.isNil)(fieldValue) || (0, _predicate.isNil)(clauseValue)) {
    return fieldValue === clauseValue;
  }

  return !greaterThen(fieldValue, clauseValue);
};

exports.lte = lte;