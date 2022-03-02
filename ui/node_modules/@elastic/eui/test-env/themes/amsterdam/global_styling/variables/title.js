"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.title_ams = void 0;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _title = require("../../../../global_styling/variables/title");

var _typography = require("../../../../global_styling/variables/_typography");

var _utils = require("../../../../services/theme/utils");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

// For Amsterdam, change all font-weights to bold and remove letter-spacing
var title_ams = _typography.SCALES.reduce(function (acc, elem) {
  acc[elem] = _objectSpread(_objectSpread({}, _title.title[elem]), {}, {
    fontWeight: (0, _utils.computed)(function (_ref) {
      var _ref2 = (0, _slicedToArray2.default)(_ref, 1),
          fontWeight = _ref2[0];

      return fontWeight;
    }, ['font.weight.bold']),
    letterSpacing: undefined
  });
  return acc;
}, {});

exports.title_ams = title_ams;