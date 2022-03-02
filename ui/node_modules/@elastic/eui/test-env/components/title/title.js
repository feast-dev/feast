"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiTitle = exports.TEXT_TRANSFORM = exports.TITLE_SIZES = void 0;

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _common = require("../common");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

var titleSizeToClassNameMap = {
  xxxs: 'euiTitle--xxxsmall',
  xxs: 'euiTitle--xxsmall',
  xs: 'euiTitle--xsmall',
  s: 'euiTitle--small',
  m: 'euiTitle--medium',
  l: 'euiTitle--large'
};
var TITLE_SIZES = (0, _common.keysOf)(titleSizeToClassNameMap);
exports.TITLE_SIZES = TITLE_SIZES;
var textTransformToClassNameMap = {
  uppercase: 'euiTitle--uppercase'
};
var TEXT_TRANSFORM = (0, _common.keysOf)(textTransformToClassNameMap);
exports.TEXT_TRANSFORM = TEXT_TRANSFORM;

var EuiTitle = function EuiTitle(_ref) {
  var _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      children = _ref.children,
      className = _ref.className,
      textTransform = _ref.textTransform,
      rest = (0, _objectWithoutProperties2.default)(_ref, ["size", "children", "className", "textTransform"]);
  var classes = (0, _classnames.default)('euiTitle', titleSizeToClassNameMap[size], textTransform ? textTransformToClassNameMap[textTransform] : undefined, className, children.props.className);

  var props = _objectSpread({
    className: classes
  }, rest);

  return /*#__PURE__*/_react.default.cloneElement(children, props);
};

exports.EuiTitle = EuiTitle;
EuiTitle.propTypes = {
  className: _propTypes.default.string,
  "aria-label": _propTypes.default.string,
  "data-test-subj": _propTypes.default.string,

  /**
     * ReactElement to render as this component's content
     */
  children: _propTypes.default.element.isRequired,
  size: _propTypes.default.oneOf(["xxxs", "xxs", "xs", "s", "m", "l"]),
  textTransform: _propTypes.default.oneOf(["uppercase"]),
  id: _propTypes.default.string
};