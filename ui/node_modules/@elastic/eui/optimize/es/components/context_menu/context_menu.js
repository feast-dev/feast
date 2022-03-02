import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _createClass from "@babel/runtime/helpers/createClass";
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
import { keysOf } from '../common';
import { EuiContextMenuPanel } from './context_menu_panel';
import { EuiContextMenuItem } from './context_menu_item';
import { EuiHorizontalRule } from '../horizontal_rule';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  s: 'euiContextMenu--small',
  m: null
};
export var SIZES = keysOf(sizeToClassNameMap);

var isItemSeparator = function isItemSeparator(item) {
  return item.isSeparator === true;
};

function mapIdsToPanels(panels) {
  var map = {};
  panels.forEach(function (panel) {
    map[panel.id] = panel;
  });
  return map;
}

function mapIdsToPreviousPanels(panels) {
  var idToPreviousPanelIdMap = {};
  panels.forEach(function (panel) {
    if (Array.isArray(panel.items)) {
      panel.items.forEach(function (item) {
        if (isItemSeparator(item)) return;
        var isCloseable = item.panel !== undefined;

        if (isCloseable) {
          idToPreviousPanelIdMap[item.panel] = panel.id;
        }
      });
    }
  });
  return idToPreviousPanelIdMap;
}

function mapPanelItemsToPanels(panels) {
  var idAndItemIndexToPanelIdMap = {};
  panels.forEach(function (panel) {
    idAndItemIndexToPanelIdMap[panel.id] = {};

    if (panel.items) {
      panel.items.forEach(function (item, index) {
        if (isItemSeparator(item)) return;

        if (item.panel) {
          idAndItemIndexToPanelIdMap[panel.id][index] = item.panel;
        }
      });
    }
  });
  return idAndItemIndexToPanelIdMap;
}

export var EuiContextMenu = /*#__PURE__*/function (_Component) {
  _inherits(EuiContextMenu, _Component);

  var _super = _createSuper(EuiContextMenu);

  _createClass(EuiContextMenu, null, [{
    key: "getDerivedStateFromProps",
    value: function getDerivedStateFromProps(nextProps, prevState) {
      var panels = nextProps.panels;

      if (panels && prevState.prevProps.panels !== panels) {
        return {
          prevProps: {
            panels: panels
          },
          idToPanelMap: mapIdsToPanels(panels),
          idToPreviousPanelIdMap: mapIdsToPreviousPanels(panels),
          idAndItemIndexToPanelIdMap: mapPanelItemsToPanels(panels)
        };
      }

      return null;
    }
  }]);

  function EuiContextMenu(props) {
    var _this;

    _classCallCheck(this, EuiContextMenu);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "hasPreviousPanel", function (panelId) {
      var previousPanelId = _this.state.idToPreviousPanelIdMap[panelId];
      return typeof previousPanelId !== 'undefined';
    });

    _defineProperty(_assertThisInitialized(_this), "showNextPanel", function (itemIndex) {
      if (itemIndex == null) {
        return;
      }

      var nextPanelId = _this.state.idAndItemIndexToPanelIdMap[_this.state.incomingPanelId][itemIndex];

      if (nextPanelId) {
        if (_this.state.isUsingKeyboardToNavigate) {
          _this.setState(function (_ref) {
            var _idToPanelMap$nextPan;

            var idToPanelMap = _ref.idToPanelMap;
            return {
              focusedItemIndex: (_idToPanelMap$nextPan = idToPanelMap[nextPanelId].initialFocusedItemIndex) !== null && _idToPanelMap$nextPan !== void 0 ? _idToPanelMap$nextPan : 0
            };
          });
        }

        _this.showPanel(nextPanelId, 'next');
      }
    });

    _defineProperty(_assertThisInitialized(_this), "showPreviousPanel", function () {
      // If there's a previous panel, then we can close the current panel to go back to it.
      if (_this.hasPreviousPanel(_this.state.incomingPanelId)) {
        var previousPanelId = _this.state.idToPreviousPanelIdMap[_this.state.incomingPanelId]; // Set focus on the item which shows the panel we're leaving.

        var previousPanel = _this.state.idToPanelMap[previousPanelId];
        var focusedItemIndex = previousPanel.items.findIndex(function (item) {
          return !isItemSeparator(item) && item.panel === _this.state.incomingPanelId;
        });

        if (focusedItemIndex !== -1) {
          _this.setState({
            focusedItemIndex: focusedItemIndex
          });
        }

        _this.showPanel(previousPanelId, 'previous');
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onIncomingPanelHeightChange", function (height) {
      _this.setState(function (_ref2) {
        var prevHeight = _ref2.height;

        if (height === prevHeight) {
          return null;
        }

        return {
          height: height
        };
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onOutGoingPanelTransitionComplete", function () {
      _this.setState({
        isOutgoingPanelVisible: false
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onUseKeyboardToNavigate", function () {
      if (!_this.state.isUsingKeyboardToNavigate) {
        _this.setState({
          isUsingKeyboardToNavigate: true
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "mapIdsToRenderedItems", function () {
      var panels = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : [];
      var idToRenderedItemsMap = {}; // Pre-rendering the items lets us check reference equality inside of EuiContextMenuPanel.

      panels.forEach(function (panel) {
        idToRenderedItemsMap[panel.id] = _this.renderItems(panel.items);
      });
      return idToRenderedItemsMap;
    });

    _this.state = {
      prevProps: {},
      idToPanelMap: {},
      idToPreviousPanelIdMap: {},
      idAndItemIndexToPanelIdMap: {},
      idToRenderedItemsMap: _this.mapIdsToRenderedItems(_this.props.panels),
      height: undefined,
      outgoingPanelId: undefined,
      incomingPanelId: props.initialPanelId,
      transitionDirection: undefined,
      isOutgoingPanelVisible: false,
      focusedItemIndex: undefined,
      isUsingKeyboardToNavigate: false
    };
    return _this;
  }

  _createClass(EuiContextMenu, [{
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      if (prevProps.panels !== this.props.panels) {
        // eslint-disable-next-line react/no-did-update-set-state
        this.setState({
          idToRenderedItemsMap: this.mapIdsToRenderedItems(this.props.panels)
        });
      }
    }
  }, {
    key: "showPanel",
    value: function showPanel(panelId, direction) {
      this.setState({
        outgoingPanelId: this.state.incomingPanelId,
        incomingPanelId: panelId,
        transitionDirection: direction,
        isOutgoingPanelVisible: true
      });
    }
  }, {
    key: "renderItems",
    value: function renderItems() {
      var _this2 = this;

      var items = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : [];
      return items.map(function (item, index) {
        if (isItemSeparator(item)) {
          var omit = item.isSeparator,
              _item$key = item.key,
              _key = _item$key === void 0 ? index : _item$key,
              _rest = _objectWithoutProperties(item, ["isSeparator", "key"]);

          return ___EmotionJSX(EuiHorizontalRule, _extends({
            key: _key,
            margin: "none"
          }, _rest));
        }

        var panel = item.panel,
            name = item.name,
            key = item.key,
            icon = item.icon,
            onClick = item.onClick,
            toolTipTitle = item.toolTipTitle,
            toolTipContent = item.toolTipContent,
            rest = _objectWithoutProperties(item, ["panel", "name", "key", "icon", "onClick", "toolTipTitle", "toolTipContent"]);

        var onClickHandler = panel ? function (event) {
          if (onClick && event) {
            event.persist();
          } // This component is commonly wrapped in a EuiOutsideClickDetector, which means we'll
          // need to wait for that logic to complete before re-rendering the DOM via showPanel.


          window.requestAnimationFrame(function () {
            if (onClick) {
              onClick(event);
            }

            _this2.showNextPanel(index);
          });
        } : onClick;
        return ___EmotionJSX(EuiContextMenuItem, _extends({
          key: key || (typeof name === 'string' ? name : undefined) || index,
          icon: icon,
          onClick: onClickHandler,
          hasPanel: Boolean(panel),
          toolTipTitle: toolTipTitle,
          toolTipContent: toolTipContent
        }, rest), name);
      });
    }
  }, {
    key: "renderPanel",
    value: function renderPanel(panelId, transitionType) {
      var _this3 = this;

      var panel = this.state.idToPanelMap[panelId];

      if (!panel) {
        return;
      } // As above, we need to wait for EuiOutsideClickDetector to complete its logic before
      // re-rendering via showPanel.


      var onClose;

      if (this.hasPreviousPanel(panelId)) {
        onClose = function onClose() {
          return window.requestAnimationFrame(_this3.showPreviousPanel);
        };
      }

      return ___EmotionJSX(EuiContextMenuPanel, {
        key: panelId,
        size: this.props.size,
        className: "euiContextMenu__panel",
        onHeightChange: transitionType === 'in' ? this.onIncomingPanelHeightChange : undefined,
        onTransitionComplete: transitionType === 'out' ? this.onOutGoingPanelTransitionComplete : undefined,
        title: panel.title,
        onClose: onClose,
        transitionType: this.state.isOutgoingPanelVisible ? transitionType : undefined,
        transitionDirection: this.state.isOutgoingPanelVisible ? this.state.transitionDirection : undefined,
        hasFocus: transitionType === 'in',
        items: this.state.idToRenderedItemsMap[panelId],
        initialFocusedItemIndex: this.state.isUsingKeyboardToNavigate ? this.state.focusedItemIndex : panel.initialFocusedItemIndex,
        onUseKeyboardToNavigate: this.onUseKeyboardToNavigate,
        showNextPanel: this.showNextPanel,
        showPreviousPanel: this.showPreviousPanel
      }, panel.content);
    }
  }, {
    key: "render",
    value: function render() {
      var _this$props = this.props,
          panels = _this$props.panels,
          className = _this$props.className,
          initialPanelId = _this$props.initialPanelId,
          size = _this$props.size,
          rest = _objectWithoutProperties(_this$props, ["panels", "className", "initialPanelId", "size"]);

      var incomingPanel = this.renderPanel(this.state.incomingPanelId, 'in');
      var outgoingPanel;

      if (this.state.isOutgoingPanelVisible) {
        outgoingPanel = this.renderPanel(this.state.outgoingPanelId, 'out');
      }

      var width = this.state.idToPanelMap[this.state.incomingPanelId] && this.state.idToPanelMap[this.state.incomingPanelId].width ? this.state.idToPanelMap[this.state.incomingPanelId].width : undefined;
      var classes = classNames('euiContextMenu', size && sizeToClassNameMap[size], className);
      return ___EmotionJSX("div", _extends({
        className: classes,
        style: {
          height: this.state.height,
          width: width
        }
      }, rest), outgoingPanel, incomingPanel);
    }
  }]);

  return EuiContextMenu;
}(Component);

_defineProperty(EuiContextMenu, "defaultProps", {
  panels: [],
  size: 'm'
});