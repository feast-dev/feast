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