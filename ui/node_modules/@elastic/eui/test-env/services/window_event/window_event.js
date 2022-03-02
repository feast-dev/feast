"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiWindowEvent = void 0;

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

var _react = require("react");

var _propTypes = _interopRequireDefault(require("prop-types"));

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var EuiWindowEvent = /*#__PURE__*/function (_Component) {
  (0, _inherits2.default)(EuiWindowEvent, _Component);

  var _super = _createSuper(EuiWindowEvent);

  function EuiWindowEvent() {
    (0, _classCallCheck2.default)(this, EuiWindowEvent);
    return _super.apply(this, arguments);
  }

  (0, _createClass2.default)(EuiWindowEvent, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      this.addEvent(this.props);
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      if (prevProps.event !== this.props.event || prevProps.handler !== this.props.handler) {
        this.removeEvent(prevProps);
        this.addEvent(this.props);
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      this.removeEvent(this.props);
    }
  }, {
    key: "addEvent",
    value: function addEvent(_ref) {
      var event = _ref.event,
          handler = _ref.handler;
      window.addEventListener(event, handler);
    }
  }, {
    key: "removeEvent",
    value: function removeEvent(_ref2) {
      var event = _ref2.event,
          handler = _ref2.handler;
      window.removeEventListener(event, handler);
    }
  }, {
    key: "render",
    value: function render() {
      return null;
    }
  }]);
  return EuiWindowEvent;
}(_react.Component);

exports.EuiWindowEvent = EuiWindowEvent;
EuiWindowEvent.propTypes = {
  event: _propTypes.default.any.isRequired,
  handler: _propTypes.default.func.isRequired
};