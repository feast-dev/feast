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
import { isString } from '../../services/predicate';
import { EuiContextMenuItem, EuiContextMenuPanel } from '../context_menu';
import { EuiPopover } from '../popover';
import { EuiButtonIcon } from '../button';
import { EuiToolTip } from '../tool_tip';
import { EuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";

function actionIsCustomItemAction(action) {
  return action.hasOwnProperty('render');
}

export var CollapsedItemActions = /*#__PURE__*/function (_Component) {
  _inherits(CollapsedItemActions, _Component);

  var _super = _createSuper(CollapsedItemActions);

  function CollapsedItemActions() {
    var _this;

    _classCallCheck(this, CollapsedItemActions);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "popoverDiv", null);

    _defineProperty(_assertThisInitialized(_this), "state", {
      popoverOpen: false
    });

    _defineProperty(_assertThisInitialized(_this), "togglePopover", function () {
      _this.setState(function (prevState) {
        return {
          popoverOpen: !prevState.popoverOpen
        };
      });
    });

    _defineProperty(_assertThisInitialized(_this), "closePopover", function () {
      _this.setState({
        popoverOpen: false
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onPopoverBlur", function () {
      // you must be asking... WTF? I know... but this timeout is
      // required to make sure we process the onBlur events after the initial
      // event cycle. Reference:
      // https://medium.com/@jessebeach/dealing-with-focus-and-blur-in-a-composite-widget-in-react-90d3c3b49a9b
      window.requestAnimationFrame(function () {
        if (!_this.popoverDiv.contains(document.activeElement) && _this.props.onBlur) {
          _this.props.onBlur();
        }
      });
    });

    _defineProperty(_assertThisInitialized(_this), "registerPopoverDiv", function (popoverDiv) {
      if (!_this.popoverDiv) {
        _this.popoverDiv = popoverDiv;

        _this.popoverDiv.addEventListener('focusout', _this.onPopoverBlur);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onClickItem", function (onClickAction) {
      _this.closePopover();

      if (onClickAction) {
        onClickAction();
      }
    });

    return _this;
  }

  _createClass(CollapsedItemActions, [{
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      if (this.popoverDiv) {
        this.popoverDiv.removeEventListener('focusout', this.onPopoverBlur);
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props = this.props,
          actions = _this$props.actions,
          itemId = _this$props.itemId,
          item = _this$props.item,
          actionEnabled = _this$props.actionEnabled,
          onFocus = _this$props.onFocus,
          className = _this$props.className;
      var isOpen = this.state.popoverOpen;
      var allDisabled = true;
      var controls = actions.reduce(function (controls, action, index) {
        var key = "action_".concat(itemId, "_").concat(index);
        var available = action.available ? action.available(item) : true;

        if (!available) {
          return controls;
        }

        var enabled = actionEnabled(action);
        allDisabled = allDisabled && !enabled;

        if (actionIsCustomItemAction(action)) {
          var customAction = action;
          var actionControl = customAction.render(item, enabled);
          var actionControlOnClick = actionControl && actionControl.props && actionControl.props.onClick;
          controls.push(___EmotionJSX(EuiContextMenuItem, {
            key: key,
            onClick: function onClick() {
              return _this2.onClickItem(actionControlOnClick ? function () {
                return actionControlOnClick(item);
              } : undefined);
            }
          }, actionControl));
        } else {
          var _onClick = action.onClick,
              name = action.name,
              href = action.href,
              target = action.target,
              dataTestSubj = action['data-test-subj'];
          var buttonIcon = action.icon;
          var icon;

          if (buttonIcon) {
            icon = isString(buttonIcon) ? buttonIcon : buttonIcon(item);
          }

          var buttonContent = typeof name === 'function' ? name(item) : name;
          controls.push(___EmotionJSX(EuiContextMenuItem, {
            key: key,
            disabled: !enabled,
            href: href,
            target: target,
            icon: icon,
            "data-test-subj": dataTestSubj,
            onClick: function onClick() {
              return _this2.onClickItem(_onClick ? function () {
                return _onClick(item);
              } : undefined);
            }
          }, buttonContent));
        }

        return controls;
      }, []);

      var popoverButton = ___EmotionJSX(EuiI18n, {
        token: "euiCollapsedItemActions.allActions",
        default: "All actions"
      }, function (allActions) {
        return ___EmotionJSX(EuiButtonIcon, {
          className: className,
          "aria-label": allActions,
          iconType: "boxesHorizontal",
          color: "text",
          isDisabled: allDisabled,
          onClick: _this2.togglePopover.bind(_this2),
          onFocus: onFocus,
          "data-test-subj": "euiCollapsedItemActionsButton"
        });
      });

      var withTooltip = !allDisabled && ___EmotionJSX(EuiI18n, {
        token: "euiCollapsedItemActions.allActions",
        default: "All actions"
      }, function (allActions) {
        return ___EmotionJSX(EuiToolTip, {
          content: allActions,
          delay: "long"
        }, popoverButton);
      });

      return ___EmotionJSX(EuiPopover, {
        className: className,
        popoverRef: this.registerPopoverDiv,
        id: "".concat(itemId, "-actions"),
        isOpen: isOpen,
        button: withTooltip || popoverButton,
        closePopover: this.closePopover,
        panelPaddingSize: "none",
        anchorPosition: "leftCenter"
      }, ___EmotionJSX(EuiContextMenuPanel, {
        items: controls
      }));
    }
  }]);

  return CollapsedItemActions;
}(Component);