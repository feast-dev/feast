function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function"); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, writable: true, configurable: true } }); if (superClass) _setPrototypeOf(subClass, superClass); }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _possibleConstructorReturn(self, call) { if (call && (_typeof(call) === "object" || typeof call === "function")) { return call; } return _assertThisInitialized(self); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { Children, cloneElement, Component } from 'react';
import PropTypes from "prop-types";
import { htmlIdGenerator } from '../../services/accessibility';
export var EuiOutsideClickDetector = /*#__PURE__*/function (_Component) {
  _inherits(EuiOutsideClickDetector, _Component);

  var _super = _createSuper(EuiOutsideClickDetector);

  // We are working with the assumption that a click event is
  // equivalent to a sequential, compound press and release of
  // the pointing device (mouse, finger, stylus, etc.).
  // A click event's target can be imprecise, as the value will be
  // the closest common ancestor of the press (mousedown, touchstart)
  // and release (mouseup, touchend) events (often <body />) if
  // the the target of each event differs.
  // We need the actual event targets to make the correct decisions
  // about user intention. So, consider the down/start and up/end
  // items below as the deconstruction of a click event.
  function EuiOutsideClickDetector(props) {
    var _this;

    _classCallCheck(this, EuiOutsideClickDetector);

    _this = _super.call(this, props); // the id is used to identify which EuiOutsideClickDetector
    // is the source of a click event; as the click event bubbles
    // up and reaches the click detector's child component the
    // id value is stamped on the event. This id is inspected
    // in the document's click handler, and if the id doesn't
    // exist or doesn't match this detector's id, then trigger
    // the outsideClick callback.
    //
    // Taking this approach instead of checking if the event's
    // target element exists in this component's DOM sub-tree is
    // necessary for handling clicks originating from children
    // rendered through React's portals (EuiPortal). The id tracking
    // works because React guarantees the event bubbles through the
    // virtual DOM and executes EuiClickDetector's onClick handler,
    // stamping the id even though the event originates outside
    // this component's reified DOM tree.

    _defineProperty(_assertThisInitialized(_this), "id", void 0);

    _defineProperty(_assertThisInitialized(_this), "capturedDownIds", void 0);

    _defineProperty(_assertThisInitialized(_this), "onClickOutside", function (e) {
      var _this$props = _this.props,
          isDisabled = _this$props.isDisabled,
          onOutsideClick = _this$props.onOutsideClick;

      if (isDisabled) {
        _this.capturedDownIds = [];
        return;
      }

      var event = e;

      if (event.euiGeneratedBy && event.euiGeneratedBy.includes(_this.id) || _this.capturedDownIds.includes(_this.id)) {
        _this.capturedDownIds = [];
        return;
      }

      _this.capturedDownIds = [];
      return onOutsideClick(event);
    });

    _defineProperty(_assertThisInitialized(_this), "onChildClick", function (event, cb) {
      // to support nested click detectors, build an array
      // of detector ids that have been encountered;
      if (event.nativeEvent.hasOwnProperty('euiGeneratedBy')) {
        event.nativeEvent.euiGeneratedBy.push(_this.id);
      } else {
        event.nativeEvent.euiGeneratedBy = [_this.id];
      }

      if (cb) cb(event);
    });

    _defineProperty(_assertThisInitialized(_this), "onChildMouseDown", function (event) {
      _this.onChildClick(event, function (e) {
        var nativeEvent = e.nativeEvent;
        _this.capturedDownIds = nativeEvent.euiGeneratedBy;
        if (_this.props.onMouseDown) _this.props.onMouseDown(e);
        if (_this.props.onTouchStart) _this.props.onTouchStart(e);
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onChildMouseUp", function (event) {
      _this.onChildClick(event, function (e) {
        if (_this.props.onMouseUp) _this.props.onMouseUp(e);
        if (_this.props.onTouchEnd) _this.props.onTouchEnd(e);
      });
    });

    _this.id = htmlIdGenerator()();
    _this.capturedDownIds = [];
    return _this;
  }

  _createClass(EuiOutsideClickDetector, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      document.addEventListener('mouseup', this.onClickOutside);
      document.addEventListener('touchend', this.onClickOutside);
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      document.removeEventListener('mouseup', this.onClickOutside);
      document.removeEventListener('touchend', this.onClickOutside);
    }
  }, {
    key: "render",
    value: function render() {
      var props = _objectSpread(_objectSpread({}, this.props.children.props), {
        onMouseDown: this.onChildMouseDown,
        onTouchStart: this.onChildMouseDown,
        onMouseUp: this.onChildMouseUp,
        onTouchEnd: this.onChildMouseUp
      });

      var child = Children.only(this.props.children);
      return /*#__PURE__*/cloneElement(child, props);
    }
  }]);

  return EuiOutsideClickDetector;
}(Component);
EuiOutsideClickDetector.propTypes = {
  /**
     * ReactNode to render as this component's content
     */
  children: PropTypes.element.isRequired,
  onOutsideClick: PropTypes.func.isRequired,
  isDisabled: PropTypes.bool,
  onMouseDown: PropTypes.func,
  onMouseUp: PropTypes.func,
  onTouchStart: PropTypes.func,
  onTouchEnd: PropTypes.func
};