function _toConsumableArray(arr) { return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _unsupportedIterableToArray(arr) || _nonIterableSpread(); }

function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _iterableToArray(iter) { if (typeof Symbol !== "undefined" && Symbol.iterator in Object(iter)) return Array.from(iter); }

function _arrayWithoutHoles(arr) { if (Array.isArray(arr)) return _arrayLikeToArray(arr); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import moment from 'moment';
import { isNil } from './predicate';
import { times } from './utils';
var defaultRand = Math.random;
export var Random = function Random() {
  var _this = this;

  var rand = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : defaultRand;

  _classCallCheck(this, Random);

  _defineProperty(this, "rand", void 0);

  _defineProperty(this, "boolean", function () {
    return _this.rand() > 0.5;
  });

  _defineProperty(this, "number", function () {
    var options = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
    var min = isNil(options.min) ? Number.MIN_VALUE : options.min;
    var max = isNil(options.max) ? Number.MAX_VALUE : options.max;
    var delta = _this.rand() * (max - min);
    return min + delta;
  });

  _defineProperty(this, "integer", function () {
    var options = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
    var min = Math.ceil(isNil(options.min) ? Number.MIN_VALUE : options.min);
    var max = Math.floor(isNil(options.max) ? Number.MAX_VALUE : options.max);
    var delta = Math.floor(_this.rand() * (max - min + 1));
    return min + delta;
  });

  _defineProperty(this, "oneOf", function (values) {
    return values[Math.floor(_this.rand() * values.length)];
  });

  _defineProperty(this, "oneToOne", function (values, index) {
    return values[index];
  });

  _defineProperty(this, "setOf", function (values) {
    var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};

    var count = _this.integer(_objectSpread({
      min: 0,
      max: values.length
    }, options));

    var copy = _toConsumableArray(values);

    return times(count, function () {
      var value = _this.oneOf(copy);

      copy.splice(copy.indexOf(value), 1);
      return value;
    });
  });

  _defineProperty(this, "date", function () {
    var options = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
    var min = isNil(options.min) ? new Date(0) : options.min;
    var max = isNil(options.max) ? new Date(Date.now()) : options.max;
    var minMls = min.getTime();
    var maxMls = max.getTime();

    var time = _this.integer({
      min: minMls,
      max: maxMls
    });

    return new Date(time);
  });

  _defineProperty(this, "moment", function () {
    var options = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
    var min = isNil(options.min) ? moment(0) : options.min;
    var max = isNil(options.max) ? moment() : options.max;
    var minMls = +min;
    var maxMls = +max;

    var time = _this.integer({
      min: minMls,
      max: maxMls
    });

    return moment(time);
  });

  this.rand = rand;
};