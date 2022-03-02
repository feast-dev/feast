function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

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
import { Component } from 'react';
import PropTypes from "prop-types";
export var EuiDelayRender = /*#__PURE__*/function (_Component) {
  _inherits(EuiDelayRender, _Component);

  var _super = _createSuper(EuiDelayRender);

  function EuiDelayRender(props) {
    var _this;

    _classCallCheck(this, EuiDelayRender);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "delayID", void 0);

    _defineProperty(_assertThisInitialized(_this), "toBeDelayed", true);

    _defineProperty(_assertThisInitialized(_this), "startDelaying", function () {
      window.clearTimeout(_this.delayID);
      _this.toBeDelayed = true;
      _this.delayID = window.setTimeout(_this.stopDelaying, _this.props.delay);
    });

    _defineProperty(_assertThisInitialized(_this), "stopDelaying", function () {
      window.clearTimeout(_this.delayID);
      _this.toBeDelayed = false;

      _this.shouldUpdate();
    });

    _this.state = {
      toggle: false
    };
    return _this;
  }

  _createClass(EuiDelayRender, [{
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
}(Component);

_defineProperty(EuiDelayRender, "defaultProps", {
  delay: 500
});

EuiDelayRender.propTypes = {
  delay: PropTypes.number.isRequired
};