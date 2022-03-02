"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _react2 = require("@emotion/react");

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var WeekNumber = /*#__PURE__*/function (_React$Component) {
  (0, _inherits2.default)(WeekNumber, _React$Component);

  var _super = _createSuper(WeekNumber);

  function WeekNumber() {
    var _this;

    (0, _classCallCheck2.default)(this, WeekNumber);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleClick", function (event) {
      if (_this.props.onClick) {
        _this.props.onClick(event);
      }
    });
    return _this;
  }

  (0, _createClass2.default)(WeekNumber, [{
    key: "render",
    value: function render() {
      var weekNumberClasses = {
        "react-datepicker__week-number": true,
        "react-datepicker__week-number--clickable": !!this.props.onClick
      };
      return (0, _react2.jsx)("div", {
        className: (0, _classnames.default)(weekNumberClasses),
        "aria-label": "week-".concat(this.props.weekNumber),
        onClick: this.handleClick
      }, this.props.weekNumber);
    }
  }]);
  return WeekNumber;
}(_react.default.Component);

exports.default = WeekNumber;
(0, _defineProperty2.default)(WeekNumber, "propTypes", {
  weekNumber: _propTypes.default.number.isRequired,
  onClick: _propTypes.default.func
});
module.exports = exports.default;