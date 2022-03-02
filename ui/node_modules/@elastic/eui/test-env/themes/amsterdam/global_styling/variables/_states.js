"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.focus_ams = void 0;

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _utils = require("../../../../services/theme/utils");

var _color = require("../../../../services/color");

var _states = require("../../../../global_styling/variables/_states");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/**
 * NOTE: These were quick conversions of their Sass counterparts.
 *       They have yet to be used/tested.
 */
var focus_ams = _objectSpread(_objectSpread({}, _states.focus), {}, {
  color: 'currentColor',
  transparency: {
    LIGHT: 0.9,
    DARK: 0.7
  },
  backgroundColor: (0, _utils.computed)(function (_ref) {
    var colors = _ref.colors,
        focus = _ref.focus;
    return (0, _color.transparentize)(colors.primary, focus.transparency);
  }),
  // Outline
  outline: {
    outline: (0, _utils.computed)(function (_ref2) {
      var focus = _ref2.focus;
      return "".concat(focus.width, " solid ").concat(focus.color);
    })
  }
});

exports.focus_ams = focus_ams;