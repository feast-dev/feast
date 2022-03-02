"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiScreenReaderOnly = void 0;

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _react = require("react");

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

var EuiScreenReaderOnly = function EuiScreenReaderOnly(_ref) {
  var children = _ref.children,
      showOnFocus = _ref.showOnFocus;
  var classes = (0, _classnames.default)({
    euiScreenReaderOnly: !showOnFocus,
    'euiScreenReaderOnly--showOnFocus': showOnFocus
  }, children.props.className);

  var props = _objectSpread(_objectSpread({}, children.props), {}, {
    className: classes
  });

  return /*#__PURE__*/(0, _react.cloneElement)(children, props);
};

exports.EuiScreenReaderOnly = EuiScreenReaderOnly;
EuiScreenReaderOnly.propTypes = {
  /**
     * ReactElement to render as this component's content
     */
  children: _propTypes.default.element.isRequired,

  /**
     * For keyboard navigation, force content to display visually upon focus.
     */
  showOnFocus: _propTypes.default.bool
};