"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.parseRelativeParts = parseRelativeParts;
exports.toRelativeStringFromParts = void 0;

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _datemath = _interopRequireDefault(require("@elastic/datemath"));

var _moment = _interopRequireDefault(require("moment"));

var _objects = require("../../../services/objects");

var _predicate = require("../../../services/predicate");

var _relative_options = require("./relative_options");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

var ROUND_DELIMETER = '/';

function parseRelativeParts(value) {
  var matches = (0, _predicate.isString)(value) && value.match(/now(([\-\+])([0-9]+)([smhdwMy])(\/[smhdwMy])?)?/);
  var operator = matches && matches[2];
  var count = matches && matches[3];
  var unit = matches && matches[4];
  var roundBy = matches && matches[5];

  if (count && unit) {
    var isRounded = roundBy ? true : false;
    var roundUnit = isRounded && roundBy ? roundBy.replace(ROUND_DELIMETER, '') : undefined;
    return _objectSpread({
      count: parseInt(count, 10),
      unit: operator === '+' ? "".concat(unit, "+") : unit,
      round: isRounded
    }, roundUnit ? {
      roundUnit: roundUnit
    } : {});
  }

  var results = {
    count: 0,
    unit: 's',
    round: false
  };

  var duration = _moment.default.duration((0, _moment.default)().diff(_datemath.default.parse(value)));

  var unitOp = '';

  for (var i = 0; i < _relative_options.relativeUnitsFromLargestToSmallest.length; i++) {
    var asRelative = duration.as(_relative_options.relativeUnitsFromLargestToSmallest[i]);
    if (asRelative < 0) unitOp = '+';

    if (Math.abs(asRelative) > 1) {
      results.count = Math.round(Math.abs(asRelative));
      results.unit = _relative_options.relativeUnitsFromLargestToSmallest[i] + unitOp;
      results.round = false;
      break;
    }
  }

  return results;
}

var toRelativeStringFromParts = function toRelativeStringFromParts(relativeParts) {
  var count = (0, _objects.get)(relativeParts, 'count', 0);
  var isRounded = (0, _objects.get)(relativeParts, 'round', false);

  if (count === 0 && !isRounded) {
    return 'now';
  }

  var matches = (0, _objects.get)(relativeParts, 'unit', 's').match(/([smhdwMy])(\+)?/);
  var unit = matches[1];
  var operator = matches && matches[2] ? matches[2] : '-';
  var round = isRounded ? "".concat(ROUND_DELIMETER).concat(unit) : '';
  return "now".concat(operator).concat(count).concat(unit).concat(round);
};

exports.toRelativeStringFromParts = toRelativeStringFromParts;