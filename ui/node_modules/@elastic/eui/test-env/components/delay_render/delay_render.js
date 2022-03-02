"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiDelayRender = void 0;

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _react = require("react");

var _propTypes = _interopRequireDefault(require("prop-types"));

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var EuiDelayRender = /*#__PURE__*/function (_Component) {
  (0, _inherits2.default)(EuiDelayRender, _Component);

  var _super = _createSuper(EuiDelayRender);

  function EuiDelayRender(props) {
    var _this;

    (0, _classCallCheck2.default)(this, EuiDelayRender);
    _this = _super.call(this, props);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "delayID", void 0);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "toBeDelayed", true);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "startDelaying", function () {
      window.clearTimeout(_this.delayID);
      _this.toBeDelayed = true;
      _this.delayID = window.setTimeout(_this.stopDelaying, _this.props.delay);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "stopDelaying", function () {
      window.clearTimeout(_this.delayID);
      _this.toBeDelayed = false;

      _this.shouldUpdate();
    });
    _this.state = {
      toggle: false
    };
    return _this;
  }

  (0, _createClass2.default)(EuiDelayRender, [{
    key: "shouldUpdate",
    value: function shouldUpdate() {
      this.setState(function (_ref) {
        var toggle = _ref.toggle;
        return {
          toggle: !toggle
        };
      });
    }
  }, {
    key: "componentDidMount",
    value: function componentDidMount() {
      this.startDelaying();
    }
  }, {
    key: "shouldComponentUpdate",
    value: function shouldComponentUpdate() {
      if (this.toBeDelayed) {
        this.startDelaying();
      }

      return true;
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      this.stopDelaying();
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate() {
      this.toBeDelayed = true;
    }
  }, {
    key: "render",
    value: function render() {
      return !this.toBeDelayed ? this.props.children : null;
    }
  }]);
  return EuiDelayRender;
}(_react.Component);

exports.EuiDelayRender = EuiDelayRender;
(0, _defineProperty2.default)(EuiDelayRender, "defaultProps", {
  delay: 500
});
EuiDelayRender.propTypes = {
  delay: _propTypes.default.number.isRequired
};