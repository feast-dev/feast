function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

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
import React, { Component } from 'react';
import PropTypes from "prop-types";
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

EuiContextMenu.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  panels: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired]).isRequired,
    title: PropTypes.node,
    items: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.node,
      key: PropTypes.string,
      panel: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired]),
      isSeparator: PropTypes.oneOf([true]),

      /**
         * Defines the width of the HR.
         */
      size: PropTypes.oneOf(["full", "half", "quarter"]),
      margin: PropTypes.oneOf(["none", "xs", "s", "m", "l", "xl", "xxl"]),
      className: PropTypes.string,
      "aria-label": PropTypes.string,
      "data-test-subj": PropTypes.string
    }).isRequired),
    content: PropTypes.node,
    width: PropTypes.number,
    initialFocusedItemIndex: PropTypes.number,

    /**
       * Alters the size of the items and the title
       */
    size: PropTypes.oneOf(["s", "m"])
  }).isRequired),
  initialPanelId: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired]),

  /**
       * Alters the size of the items and the title
       */
  size: PropTypes.oneOf(["s", "m"])
};