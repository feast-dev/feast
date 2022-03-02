"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useResizeObserver = exports.EuiResizeObserver = exports.hasResizeObserver = void 0;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _react = require("react");

var _observer = require("../observer");

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var hasResizeObserver = typeof window !== 'undefined' && typeof window.ResizeObserver !== 'undefined';
exports.hasResizeObserver = hasResizeObserver;

var EuiResizeObserver = /*#__PURE__*/function (_EuiObserver) {
  (0, _inherits2.default)(EuiResizeObserver, _EuiObserver);

  var _super = _createSuper(EuiResizeObserver);

  function EuiResizeObserver() {
    var _this;

    (0, _classCallCheck2.default)(this, EuiResizeObserver);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "name", 'EuiResizeObserver');
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "state", {
      height: 0,
      width: 0
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onResize", function () {
      // `entry.contentRect` provides incomplete `height` and `width` data.
      // Use `getBoundingClientRect` to account for padding and border.
      // https://developer.mozilla.org/en-US/docs/Web/API/DOMRectReadOnly
      if (!_this.childNode) return;

      var _this$childNode$getBo = _this.childNode.getBoundingClientRect(),
          height = _this$childNode$getBo.height,
          width = _this$childNode$getBo.width; // Check for actual resize event


      if (_this.state.height === height && _this.state.width === width) {
        return;
      }

      _this.props.onResize({
        height: height,
        width: width
      });

      _this.setState({
        height: height,
        width: width
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "beginObserve", function () {
      // The superclass checks that childNode is not null before invoking
      // beginObserve()
      var childNode = _this.childNode;
      _this.observer = makeResizeObserver(childNode, _this.onResize);
    });
    return _this;
  }

  return EuiResizeObserver;
}(_observer.EuiObserver);

exports.EuiResizeObserver = EuiResizeObserver;

var makeResizeObserver = function makeResizeObserver(node, callback) {
  var observer;

  if (hasResizeObserver) {
    observer = new window.ResizeObserver(callback);
    observer.observe(node);
  }

  return observer;
};

var useResizeObserver = function useResizeObserver(container, dimension) {
  var _useState = (0, _react.useState)({
    width: 0,
    height: 0
  }),
      _useState2 = (0, _slicedToArray2.default)(_useState, 2),
      size = _useState2[0],
      _setSize = _useState2[1]; // _currentDimensions and _setSize are used to only store the
  // new state (and trigger a re-render) when the new dimensions actually differ


  var _currentDimensions = (0, _react.useRef)(size);

  var setSize = (0, _react.useCallback)(function (dimensions) {
    var doesWidthMatter = dimension !== 'height';
    var doesHeightMatter = dimension !== 'width';

    if (doesWidthMatter && _currentDimensions.current.width !== dimensions.width || doesHeightMatter && _currentDimensions.current.height !== dimensions.height) {
      _currentDimensions.current = dimensions;

      _setSize(dimensions);
    }
  }, [dimension]);
  (0, _react.useEffect)(function () {
    if (container != null) {
      // ResizeObserver's first call to the observation callback is scheduled in the future
      // so find the container's initial dimensions now
      var boundingRect = container.getBoundingClientRect();
      setSize({
        width: boundingRect.width,
        height: boundingRect.height
      });
      var observer = makeResizeObserver(container, function () {
        // `entry.contentRect` provides incomplete `height` and `width` data.
        // Use `getBoundingClientRect` to account for padding and border.
        // https://developer.mozilla.org/en-US/docs/Web/API/DOMRectReadOnly
        var _container$getBoundin = container.getBoundingClientRect(),
            height = _container$getBoundin.height,
            width = _container$getBoundin.width;

        setSize({
          width: width,
          height: height
        });
      });
      return function () {
        return observer && observer.disconnect();
      };
    } else {
      setSize({
        width: 0,
        height: 0
      });
    }
  }, [container, setSize]);
  return size;
};

exports.useResizeObserver = useResizeObserver;