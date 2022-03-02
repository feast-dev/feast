import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { Component } from 'react';

function isComponentBecomingVisible() {
  var prevHide = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;
  var nextHide = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
  return prevHide === true && nextHide === false;
}

export var EuiDelayHide = /*#__PURE__*/function (_Component) {
  _inherits(EuiDelayHide, _Component);

  var _super = _createSuper(EuiDelayHide);

  function EuiDelayHide() {
    var _this;

    _classCallCheck(this, EuiDelayHide);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "state", {
      hide: _this.props.hide,
      countdownExpired: _this.props.hide
    });

    _defineProperty(_assertThisInitialized(_this), "timeoutId", void 0);

    _defineProperty(_assertThisInitialized(_this), "startCountdown", function () {
      // only start the countdown if there is not one in progress
      if (_this.timeoutId == null) {
        _this.timeoutId = setTimeout(_this.finishCountdown, // even though `minimumDuration` cannot be undefined, passing a strict number type to setTimeout makes TS interpret
        // it as a NodeJS.Timer instead of a number. The DOM lib defines the setTimeout call as taking `number | undefined`
        // so we cast minimumDuration to this type instead to force TS's cooperation
        _this.props.minimumDuration);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "finishCountdown", function () {
      _this.timeoutId = undefined;

      _this.setState({
        countdownExpired: true
      });
    });

    return _this;
  }

  _createClass(EuiDelayHide, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      // if the component begins visible start counting
      if (this.props.hide === false) {
        this.startCountdown();
      }
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      var isBecomingVisible = isComponentBecomingVisible(prevProps.hide, this.props.hide);

      if (isBecomingVisible) {
        this.startCountdown();
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      if (this.timeoutId != null) {
        clearTimeout(this.timeoutId);
      }
    }
  }, {
    key: "render",
    value: function render() {
      var shouldHideContent = this.props.hide === true && this.state.countdownExpired;
      return shouldHideContent ? null : this.props.render();
    }
  }], [{
    key: "getDerivedStateFromProps",
    value: function getDerivedStateFromProps(nextProps, prevState) {
      var isBecomingVisible = isComponentBecomingVisible(prevState.hide, nextProps.hide);
      return {
        hide: nextProps.hide,
        countdownExpired: isBecomingVisible ? false : prevState.countdownExpired
      };
    }
  }]);

  return EuiDelayHide;
}(Component);

_defineProperty(EuiDelayHide, "defaultProps", {
  hide: false,
  minimumDuration: 1000
});