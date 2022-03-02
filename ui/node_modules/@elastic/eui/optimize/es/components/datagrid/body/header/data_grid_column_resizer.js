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
import React, { Component } from 'react';
import { jsx as ___EmotionJSX } from "@emotion/react";
var MINIMUM_COLUMN_WIDTH = 40;
export var EuiDataGridColumnResizer = /*#__PURE__*/function (_Component) {
  _inherits(EuiDataGridColumnResizer, _Component);

  var _super = _createSuper(EuiDataGridColumnResizer);

  function EuiDataGridColumnResizer() {
    var _this;

    _classCallCheck(this, EuiDataGridColumnResizer);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "state", {
      initialX: 0,
      offset: 0
    });

    _defineProperty(_assertThisInitialized(_this), "onMouseDown", function (e) {
      _this.setState({
        initialX: e.pageX
      });

      window.addEventListener('mouseup', _this.onMouseUp);
      window.addEventListener('blur', _this.onMouseUp);
      window.addEventListener('mousemove', _this.onMouseMove); // don't let this action steal focus

      e.preventDefault();
    });

    _defineProperty(_assertThisInitialized(_this), "onMouseUp", function () {
      var offset = _this.state.offset;
      var _this$props = _this.props,
          columnId = _this$props.columnId,
          columnWidth = _this$props.columnWidth,
          setColumnWidth = _this$props.setColumnWidth;
      setColumnWidth(columnId, Math.max(MINIMUM_COLUMN_WIDTH, columnWidth + offset));

      _this.setState({
        offset: 0
      });

      window.removeEventListener('mouseup', _this.onMouseUp);
      window.removeEventListener('blur', _this.onMouseUp);
      window.removeEventListener('mousemove', _this.onMouseMove);
    });

    _defineProperty(_assertThisInitialized(_this), "onMouseMove", function (e) {
      var columnWidth = _this.props.columnWidth;

      _this.setState(function (_ref) {
        var initialX = _ref.initialX;
        return {
          offset: Math.max(e.pageX - initialX, -(columnWidth - MINIMUM_COLUMN_WIDTH))
        };
      });
    });

    return _this;
  }

  _createClass(EuiDataGridColumnResizer, [{
    key: "render",
    value: function render() {
      var offset = this.state.offset;
      return ___EmotionJSX("div", {
        className: "euiDataGridColumnResizer",
        "data-test-subj": "dataGridColumnResizer",
        style: {
          marginRight: "".concat(-offset, "px")
        },
        onMouseDown: this.onMouseDown
      });
    }
  }]);

  return EuiDataGridColumnResizer;
}(Component);