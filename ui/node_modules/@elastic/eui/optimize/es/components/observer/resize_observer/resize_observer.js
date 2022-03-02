import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
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
import { useCallback, useEffect, useRef, useState } from 'react';
import { EuiObserver } from '../observer';
export var hasResizeObserver = typeof window !== 'undefined' && typeof window.ResizeObserver !== 'undefined';
export var EuiResizeObserver = /*#__PURE__*/function (_EuiObserver) {
  _inherits(EuiResizeObserver, _EuiObserver);

  var _super = _createSuper(EuiResizeObserver);

  function EuiResizeObserver() {
    var _this;

    _classCallCheck(this, EuiResizeObserver);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "name", 'EuiResizeObserver');

    _defineProperty(_assertThisInitialized(_this), "state", {
      height: 0,
      width: 0
    });

    _defineProperty(_assertThisInitialized(_this), "onResize", function () {
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

    _defineProperty(_assertThisInitialized(_this), "beginObserve", function () {
      // The superclass checks that childNode is not null before invoking
      // beginObserve()
      var childNode = _this.childNode;
      _this.observer = makeResizeObserver(childNode, _this.onResize);
    });

    return _this;
  }

  return EuiResizeObserver;
}(EuiObserver);

var makeResizeObserver = function makeResizeObserver(node, callback) {
  var observer;

  if (hasResizeObserver) {
    observer = new window.ResizeObserver(callback);
    observer.observe(node);
  }

  return observer;
};

export var useResizeObserver = function useResizeObserver(container, dimension) {
  var _useState = useState({
    width: 0,
    height: 0
  }),
      _useState2 = _slicedToArray(_useState, 2),
      size = _useState2[0],
      _setSize = _useState2[1]; // _currentDimensions and _setSize are used to only store the
  // new state (and trigger a re-render) when the new dimensions actually differ


  var _currentDimensions = useRef(size);

  var setSize = useCallback(function (dimensions) {
    var doesWidthMatter = dimension !== 'height';
    var doesHeightMatter = dimension !== 'width';

    if (doesWidthMatter && _currentDimensions.current.width !== dimensions.width || doesHeightMatter && _currentDimensions.current.height !== dimensions.height) {
      _currentDimensions.current = dimensions;

      _setSize(dimensions);
    }
  }, [dimension]);
  useEffect(function () {
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