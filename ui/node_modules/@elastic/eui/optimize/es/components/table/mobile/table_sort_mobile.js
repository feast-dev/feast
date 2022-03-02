import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
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
import classNames from 'classnames';
import { EuiButtonEmpty } from '../../button/button_empty';
import { EuiPopover } from '../../popover';
import { EuiContextMenuPanel } from '../../context_menu';
import { EuiI18n } from '../../i18n';
import { EuiTableSortMobileItem } from './table_sort_mobile_item';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiTableSortMobile = /*#__PURE__*/function (_Component) {
  _inherits(EuiTableSortMobile, _Component);

  var _super = _createSuper(EuiTableSortMobile);

  function EuiTableSortMobile() {
    var _this;

    _classCallCheck(this, EuiTableSortMobile);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "state", {
      isPopoverOpen: false
    });

    _defineProperty(_assertThisInitialized(_this), "onButtonClick", function () {
      _this.setState({
        isPopoverOpen: !_this.state.isPopoverOpen
      });
    });

    _defineProperty(_assertThisInitialized(_this), "closePopover", function () {
      _this.setState({
        isPopoverOpen: false
      });
    });

    return _this;
  }

  _createClass(EuiTableSortMobile, [{
    key: "render",
    value: function render() {
      var _this$props = this.props,
          className = _this$props.className,
          anchorPosition = _this$props.anchorPosition,
          items = _this$props.items,
          rest = _objectWithoutProperties(_this$props, ["className", "anchorPosition", "items"]);

      var classes = classNames('euiTableSortMobile', className);

      var mobileSortButton = ___EmotionJSX(EuiButtonEmpty, {
        iconType: "arrowDown",
        iconSide: "right",
        onClick: this.onButtonClick.bind(this),
        flush: "right",
        size: "xs"
      }, ___EmotionJSX(EuiI18n, {
        token: "euiTableSortMobile.sorting",
        default: "Sorting"
      }));

      var mobileSortPopover = ___EmotionJSX(EuiPopover, _extends({
        button: mobileSortButton,
        isOpen: this.state.isPopoverOpen,
        closePopover: this.closePopover,
        anchorPosition: anchorPosition || 'downRight',
        panelPaddingSize: "none"
      }, rest), ___EmotionJSX(EuiContextMenuPanel, {
        style: {
          minWidth: 200
        },
        items: items && items.length ? items.map(function (item) {
          return ___EmotionJSX(EuiTableSortMobileItem, {
            key: item.key,
            onSort: item.onSort,
            isSorted: item.isSorted,
            isSortAscending: item.isSortAscending
          }, item.name);
        }) : undefined,
        watchedItemProps: ['isSorted', 'isSortAscending']
      }));

      return ___EmotionJSX("div", {
        className: classes
      }, mobileSortPopover);
    }
  }]);

  return EuiTableSortMobile;
}(Component);