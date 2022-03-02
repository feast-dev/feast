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
import React, { cloneElement, Component } from 'react';
import classNames from 'classnames';
import tabbable from 'tabbable';
import { keysOf } from '../common';
import { EuiIcon } from '../icon';
import { EuiResizeObserver } from '../observer/resize_observer';
import { cascadingMenuKeys } from '../../services';
import { EuiContextMenuItem } from './context_menu_item';
import { jsx as ___EmotionJSX } from "@emotion/react";
var titleSizeToClassNameMap = {
  s: 'euiContextMenuPanelTitle--small',
  m: null
};
export var SIZES = keysOf(titleSizeToClassNameMap);
var transitionDirectionAndTypeToClassNameMap = {
  next: {
    in: 'euiContextMenuPanel-txInLeft',
    out: 'euiContextMenuPanel-txOutLeft'
  },
  previous: {
    in: 'euiContextMenuPanel-txInRight',
    out: 'euiContextMenuPanel-txOutRight'
  }
};
export var EuiContextMenuPanel = /*#__PURE__*/function (_Component) {
  _inherits(EuiContextMenuPanel, _Component);

  var _super = _createSuper(EuiContextMenuPanel);

  function EuiContextMenuPanel(props) {
    var _this;

    _classCallCheck(this, EuiContextMenuPanel);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "_isMounted", false);

    _defineProperty(_assertThisInitialized(_this), "backButton", null);

    _defineProperty(_assertThisInitialized(_this), "content", null);

    _defineProperty(_assertThisInitialized(_this), "panel", null);

    _defineProperty(_assertThisInitialized(_this), "incrementFocusedItemIndex", function (amount) {
      var nextFocusedItemIndex;

      if (_this.state.focusedItemIndex === undefined) {
        // If this is the beginning of the user's keyboard navigation of the menu, then we'll focus
        // either the first or last item.
        nextFocusedItemIndex = amount < 0 ? _this.state.menuItems.length - 1 : 0;
      } else {
        nextFocusedItemIndex = _this.state.focusedItemIndex + amount;

        if (nextFocusedItemIndex < 0) {
          nextFocusedItemIndex = _this.state.menuItems.length - 1;
        } else if (nextFocusedItemIndex === _this.state.menuItems.length) {
          nextFocusedItemIndex = 0;
        }
      }

      _this.setState({
        focusedItemIndex: nextFocusedItemIndex
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onKeyDown", function (event) {
      // If this panel contains items you can use the left arrow key to go back at any time.
      // But if it doesn't contain items, then you have to focus on the back button specifically,
      // since there could be content inside the panel which requires use of the left arrow key,
      // e.g. text inputs.
      var _this$props = _this.props,
          items = _this$props.items,
          showPreviousPanel = _this$props.showPreviousPanel;

      if (items && items.length || document.activeElement === _this.backButton || document.activeElement === _this.panel) {
        if (event.key === cascadingMenuKeys.ARROW_LEFT) {
          if (showPreviousPanel) {
            event.preventDefault();
            event.stopPropagation();
            showPreviousPanel();

            if (_this.props.onUseKeyboardToNavigate) {
              _this.props.onUseKeyboardToNavigate();
            }
          }
        }
      }

      if (_this.props.items && _this.props.items.length) {
        switch (event.key) {
          case cascadingMenuKeys.TAB:
            // We need to sync up with the user if s/he is tabbing through the items.
            var focusedItemIndex = _this.state.menuItems.indexOf(document.activeElement);

            _this.setState({
              focusedItemIndex: focusedItemIndex >= 0 && focusedItemIndex < _this.state.menuItems.length ? focusedItemIndex : undefined
            });

            break;

          case cascadingMenuKeys.ARROW_UP:
            event.preventDefault();

            _this.incrementFocusedItemIndex(-1);

            if (_this.props.onUseKeyboardToNavigate) {
              _this.props.onUseKeyboardToNavigate();
            }

            break;

          case cascadingMenuKeys.ARROW_DOWN:
            event.preventDefault();

            _this.incrementFocusedItemIndex(1);

            if (_this.props.onUseKeyboardToNavigate) {
              _this.props.onUseKeyboardToNavigate();
            }

            break;

          case cascadingMenuKeys.ARROW_RIGHT:
            if (_this.props.showNextPanel) {
              event.preventDefault();

              _this.props.showNextPanel(_this.state.focusedItemIndex);

              if (_this.props.onUseKeyboardToNavigate) {
                _this.props.onUseKeyboardToNavigate();
              }
            }

            break;

          default:
            break;
        }
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onTransitionComplete", function () {
      if (_this.props.onTransitionComplete) {
        _this.props.onTransitionComplete();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "menuItemRef", function (index, node) {
      // There's a weird bug where if you navigate to a panel without items, then this callback
      // is still invoked, so we have to do a truthiness check.
      if (node) {
        // Store all menu items.
        _this.state.menuItems[index] = node;
      }
    });

    _defineProperty(_assertThisInitialized(_this), "panelRef", function (node) {
      _this.panel = node;

      _this.updateHeight();
    });

    _defineProperty(_assertThisInitialized(_this), "contentRef", function (node) {
      _this.content = node;
    });

    _this.state = {
      prevProps: {
        items: _this.props.items
      },
      menuItems: [],
      focusedItemIndex: props.initialFocusedItemIndex,
      currentHeight: undefined
    };
    return _this;
  }

  _createClass(EuiContextMenuPanel, [{
    key: "updateFocus",
    value: function updateFocus() {
      var _this2 = this;

      // Give positioning time to render before focus is applied. Otherwise page jumps.
      requestAnimationFrame(function () {
        if (!_this2._isMounted) {
          return;
        } // If this panel has lost focus, then none of its content should be focused.


        if (!_this2.props.hasFocus) {
          if (_this2.panel && _this2.panel.contains(document.activeElement)) {
            document.activeElement.blur();
          }

          return;
        } // Setting focus while transitioning causes the animation to glitch, so we have to wait
        // until it's finished before we focus anything.


        if (_this2.props.transitionType) {
          return;
        } // `focusedItemIndex={-1}` specifies that the panel itself should be focused.
        // This should only be used when the panel does not have `item`s
        // and preventing autofocus is desired, which is an uncommon case.


        if (_this2.panel && _this2.state.focusedItemIndex === -1) {
          _this2.panel.focus();

          return;
        } // If there aren't any items then this is probably a form or something.


        if (!_this2.state.menuItems.length) {
          // If we've already focused on something inside the panel, everything's fine.
          if (_this2.panel && _this2.panel.contains(document.activeElement)) {
            return;
          } // Otherwise let's focus the first tabbable item and expedite input from the user.


          if (_this2.content) {
            var tabbableItems = tabbable(_this2.content);

            if (tabbableItems.length) {
              tabbableItems[0].focus();
            }
          }

          return;
        } // If an item is focused, focus it.


        if (_this2.state.focusedItemIndex !== undefined) {
          _this2.state.menuItems[_this2.state.focusedItemIndex].focus();

          return;
        } // Focus on the panel as a last resort.


        if (_this2.panel && !_this2.panel.contains(document.activeElement)) {
          _this2.panel.focus();
        }
      });
    }
  }, {
    key: "componentDidMount",
    value: function componentDidMount() {
      this.updateFocus();
      this._isMounted = true;
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      this._isMounted = false;
    }
  }, {
    key: "getWatchedPropsForItems",
    value: function getWatchedPropsForItems(items) {
      // This lets us compare prevProps and nextProps among items so we can re-render if our items
      // have changed.
      var watchedItemProps = this.props.watchedItemProps; // Create fingerprint of all item's watched properties

      if (items.length && watchedItemProps && watchedItemProps.length) {
        return JSON.stringify(items.map(function (item) {
          // Create object of item properties and values
          var props = {
            key: item.key
          };
          watchedItemProps.forEach(function (prop) {
            props[prop] = item.props[prop];
          });
          return props;
        }));
      }

      return null;
    }
  }, {
    key: "didItemsChange",
    value: function didItemsChange(prevItems, nextItems) {
      // If the count of items has changed then update
      if (prevItems.length !== nextItems.length) {
        return true;
      } // Check if any watched item properties changed by quick string comparison


      if (this.getWatchedPropsForItems(nextItems) !== this.getWatchedPropsForItems(prevItems)) {
        return true;
      }
    }
  }, {
    key: "shouldComponentUpdate",
    value: function shouldComponentUpdate(nextProps, nextState) {
      // Prevent calling `this.updateFocus()` below if we don't have to.
      if (nextProps.hasFocus !== this.props.hasFocus) {
        return true;
      }

      if (nextProps.transitionType !== this.props.transitionType) {
        return true;
      }

      if (nextState.focusedItemIndex !== this.state.focusedItemIndex) {
        return true;
      } // **
      // this component should have either items or children,
      // if there are items we can determine via `watchedItemProps` if we should update
      // if there are children we can't know if they have changed so return true
      // **


      if (this.props.items && this.props.items.length > 0 || nextProps.items && nextProps.items.length > 0) {
        if (this.didItemsChange(this.props.items, nextProps.items)) {
          return true;
        }
      } // it's not possible (in any good way) to know if `children` has changed, assume they might have


      if (this.props.children != null) {
        return true;
      }

      return false;
    }
  }, {
    key: "updateHeight",
    value: function updateHeight() {
      var currentHeight = this.panel ? this.panel.clientHeight : 0;

      if (this.state.height !== currentHeight) {
        if (this.props.onHeightChange) {
          this.props.onHeightChange(currentHeight);
          this.setState({
            height: currentHeight
          });
        }
      }
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate() {
      this.updateFocus();
    }
  }, {
    key: "render",
    value: function render() {
      var _this3 = this;

      var _this$props2 = this.props,
          children = _this$props2.children,
          className = _this$props2.className,
          onClose = _this$props2.onClose,
          title = _this$props2.title,
          onHeightChange = _this$props2.onHeightChange,
          transitionType = _this$props2.transitionType,
          transitionDirection = _this$props2.transitionDirection,
          onTransitionComplete = _this$props2.onTransitionComplete,
          onUseKeyboardToNavigate = _this$props2.onUseKeyboardToNavigate,
          hasFocus = _this$props2.hasFocus,
          items = _this$props2.items,
          watchedItemProps = _this$props2.watchedItemProps,
          initialFocusedItemIndex = _this$props2.initialFocusedItemIndex,
          showNextPanel = _this$props2.showNextPanel,
          showPreviousPanel = _this$props2.showPreviousPanel,
          size = _this$props2.size,
          rest = _objectWithoutProperties(_this$props2, ["children", "className", "onClose", "title", "onHeightChange", "transitionType", "transitionDirection", "onTransitionComplete", "onUseKeyboardToNavigate", "hasFocus", "items", "watchedItemProps", "initialFocusedItemIndex", "showNextPanel", "showPreviousPanel", "size"]);

      var panelTitle;

      if (title) {
        var titleClasses = classNames('euiContextMenuPanelTitle', size && titleSizeToClassNameMap[size]);

        if (Boolean(onClose)) {
          panelTitle = ___EmotionJSX("button", {
            className: titleClasses,
            type: "button",
            onClick: onClose,
            ref: function ref(node) {
              _this3.backButton = node;
            },
            "data-test-subj": "contextMenuPanelTitleButton"
          }, ___EmotionJSX("span", {
            className: "euiContextMenu__itemLayout"
          }, ___EmotionJSX(EuiIcon, {
            type: "arrowLeft",
            size: "m",
            className: "euiContextMenu__icon"
          }), ___EmotionJSX("span", {
            className: "euiContextMenu__text"
          }, title)));
        } else {
          panelTitle = ___EmotionJSX("div", {
            className: titleClasses
          }, ___EmotionJSX("span", {
            className: "euiContextMenu__itemLayout"
          }, title));
        }
      }

      var classes = classNames('euiContextMenuPanel', className, transitionDirection && transitionType && transitionDirectionAndTypeToClassNameMap[transitionDirection] ? transitionDirectionAndTypeToClassNameMap[transitionDirection][transitionType] : undefined);
      var content = items && items.length ? items.map(function (MenuItem, index) {
        var cloneProps = {
          buttonRef: function buttonRef(node) {
            return _this3.menuItemRef(index, node);
          }
        };

        if (size) {
          cloneProps.size = size;
        }

        return MenuItem.type === EuiContextMenuItem ? /*#__PURE__*/cloneElement(MenuItem, cloneProps) : MenuItem;
      }) : children;
      return ___EmotionJSX("div", _extends({
        ref: this.panelRef,
        className: classes,
        onKeyDown: this.onKeyDown,
        tabIndex: -1,
        onAnimationEnd: this.onTransitionComplete
      }, rest), panelTitle, ___EmotionJSX("div", {
        ref: this.contentRef
      }, ___EmotionJSX(EuiResizeObserver, {
        onResize: function onResize() {
          return _this3.updateHeight();
        }
      }, function (resizeRef) {
        return ___EmotionJSX("div", {
          ref: resizeRef
        }, content);
      })));
    }
  }], [{
    key: "getDerivedStateFromProps",
    value: function getDerivedStateFromProps(nextProps, prevState) {
      var needsUpdate = false;
      var nextState = {}; // Clear refs to menuItems if we're getting new ones.

      if (nextProps.items !== prevState.prevProps.items) {
        needsUpdate = true;
        nextState.menuItems = [];
        nextState.prevProps = {
          items: nextProps.items
        };
      }

      if (needsUpdate) {
        return nextState;
      }

      return null;
    }
  }]);

  return EuiContextMenuPanel;
}(Component);

_defineProperty(EuiContextMenuPanel, "defaultProps", {
  hasFocus: true,
  items: []
});