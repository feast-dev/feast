"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.Random = void 0;

var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _moment = _interopRequireDefault(require("moment"));

var _predicate = require("./predicate");

var _utils = require("./utils");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

var defaultRand = Math.random;

var Random = function Random() {
  var _this = this;

  var rand = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : defaultRand;
  (0, _classCallCheck2.default)(this, Random);
  (0, _defineProperty2.default)(this, "rand", void 0);
  (0, _defineProperty2.default)(this, "boolean", function () {
    return _this.rand() > 0.5;
  });
  (0, _defineProperty2.default)(this, "number", function () {
    var options = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
    var min = (0, _predicate.isNil)(options.min) ? Number.MIN_VALUE : options.min;
    var max = (0, _predicate.isNil)(options.max) ? Number.MAX_VALUE : options.max;
    var delta = _this.rand() * (max - min);
    return min + delta;
  });
  (0, _defineProperty2.default)(this, "integer", function () {
    var options = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
    var min = Math.ceil((0, _predicate.isNil)(options.min) ? Number.MIN_VALUE : options.min);
    var max = Math.floor((0, _predicate.isNil)(options.max) ? Number.MAX_VALUE : options.max);
    var delta = Math.floor(_this.rand() * (max - min + 1));
    return min + delta;
  });
  (0, _defineProperty2.default)(this, "oneOf", function (values) {
    return values[Math.floor(_this.rand() * values.length)];
  });
  (0, _defineProperty2.default)(this, "oneToOne", function (values, index) {
    return values[index];
  });
  (0, _defineProperty2.default)(this, "setOf", function (values) {
    var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};

    var count = _this.integer(_objectSpread({
      min: 0,
      max: values.length
    }, options));

    var copy = (0, _toConsumableArray2.default)(values);
    return (0, _utils.times)(count, function () {
      var value = _this.oneOf(copy);

      copy.splice(copy.indexOf(value), 1);
      return value;
    });
  });
  (0, _defineProperty2.default)(this, "date", function () {
    var options = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
    var min = (0, _predicate.isNil)(options.min) ? new Date(0) : options.min;
    var max = (0, _predicate.isNil)(options.max) ? new Date(Date.now()) : options.max;
    var minMls = min.getTime();
    var maxMls = max.getTime();

    var time = _this.integer({
      min: minMls,
      max: maxMls
    });

    return new Date(time);
  });
  (0, _defineProperty2.default)(this, "moment", function () {
    var options = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
    var min = (0, _predicate.isNil)(options.min) ? (0, _moment.default)(0) : options.min;
    var max = (0, _predicate.isNil)(options.max) ? (0, _moment.default)() : options.max;
    var minMls = +min;
    var maxMls = +max;

    var time = _this.integer({
      min: minMls,
      max: maxMls
    });

    return (0, _moment.default)(time);
  });
  this.rand = rand;
};

exports.Random = Random;