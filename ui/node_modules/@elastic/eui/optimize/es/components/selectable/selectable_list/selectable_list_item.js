import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
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
import classNames from 'classnames';
import React, { Component } from 'react';
import { EuiI18n } from '../../i18n';
import { EuiIcon } from '../../icon';
import { EuiScreenReaderOnly } from '../../accessibility';
import { EuiBadge } from '../../badge';
import { jsx as ___EmotionJSX } from "@emotion/react";

function resolveIconAndColor(checked) {
  if (!checked) {
    return {
      icon: 'empty'
    };
  }

  return checked === 'on' ? {
    icon: 'check',
    color: 'text'
  } : {
    icon: 'cross',
    color: 'text'
  };
}

// eslint-disable-next-line react/prefer-stateless-function
export var EuiSelectableListItem = /*#__PURE__*/function (_Component) {
  _inherits(EuiSelectableListItem, _Component);

  var _super = _createSuper(EuiSelectableListItem);

  function EuiSelectableListItem(props) {
    _classCallCheck(this, EuiSelectableListItem);

    return _super.call(this, props);
  }

  _createClass(EuiSelectableListItem, [{
    key: "render",
    value: function render() {
      var _this$props = this.props,
          children = _this$props.children,
          className = _this$props.className,
          disabled = _this$props.disabled,
          checked = _this$props.checked,
          isFocused = _this$props.isFocused,
          showIcons = _this$props.showIcons,
          prepend = _this$props.prepend,
          append = _this$props.append,
          allowExclusions = _this$props.allowExclusions,
          onFocusBadge = _this$props.onFocusBadge,
          rest = _objectWithoutProperties(_this$props, ["children", "className", "disabled", "checked", "isFocused", "showIcons", "prepend", "append", "allowExclusions", "onFocusBadge"]);

      var classes = classNames('euiSelectableListItem', {
        'euiSelectableListItem-isFocused': isFocused
      }, className);
      var optionIcon;

      if (showIcons) {
        var _resolveIconAndColor = resolveIconAndColor(checked),
            icon = _resolveIconAndColor.icon,
            color = _resolveIconAndColor.color;

        optionIcon = ___EmotionJSX(EuiIcon, {
          className: "euiSelectableListItem__icon",
          color: color,
          type: icon
        });
      }

      var state;
      var instruction;

      if (allowExclusions && checked === 'on') {
        state = ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, ___EmotionJSX(EuiI18n, {
          token: "euiSelectableListItem.includedOption",
          default: "Included option."
        })));
        instruction = ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, ___EmotionJSX(EuiI18n, {
          token: "euiSelectableListItem.includedOptionInstructions",
          default: "To exclude this option, press enter."
        })));
      } else if (allowExclusions && checked === 'off') {
        state = ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, ___EmotionJSX(EuiI18n, {
          token: "euiSelectableListItem.excludedOption",
          default: "Excluded option."
        })));
        instruction = ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, ___EmotionJSX(EuiI18n, {
          token: "euiSelectableListItem.excludedOptionInstructions",
          default: "To deselect this option, press enter."
        })));
      }

      var prependNode;

      if (prepend) {
        prependNode = ___EmotionJSX("span", {
          className: "euiSelectableListItem__prepend"
        }, prepend);
      }

      var appendNode;

      if (append || !!onFocusBadge) {
        var onFocusBadgeNode;
        var defaultOnFocusBadgeProps = {
          'aria-hidden': true,
          iconType: 'returnKey',
          iconSide: 'left',
          color: 'hollow'
        };

        if (onFocusBadge === true) {
          onFocusBadgeNode = ___EmotionJSX(EuiBadge, _extends({
            className: "euiSelectableListItem__onFocusBadge"
          }, defaultOnFocusBadgeProps));
        } else if (!!onFocusBadge && onFocusBadge !== false) {
          var _children = onFocusBadge.children,
              _className = onFocusBadge.className,
              restBadgeProps = _objectWithoutProperties(onFocusBadge, ["children", "className"]);

          onFocusBadgeNode = ___EmotionJSX(EuiBadge, _extends({
            className: classNames('euiSelectableListItem__onFocusBadge', _className)
          }, defaultOnFocusBadgeProps, restBadgeProps), _children);
        } // Only display the append wrapper if append exists or isFocused


        if (append || isFocused && !disabled) {
          appendNode = ___EmotionJSX("span", {
            className: "euiSelectableListItem__append"
          }, append, " ", isFocused && !disabled ? onFocusBadgeNode : null);
        }
      }

      return ___EmotionJSX("li", _extends({
        // eslint-disable-next-line jsx-a11y/no-noninteractive-element-to-interactive-role
        role: "option",
        "aria-selected": !disabled && typeof checked === 'string',
        className: classes,
        "aria-disabled": disabled
      }, rest), ___EmotionJSX("span", {
        className: "euiSelectableListItem__content"
      }, optionIcon, prependNode, ___EmotionJSX("span", {
        className: "euiSelectableListItem__text"
      }, state, children, instruction), appendNode));
    }
  }]);

  return EuiSelectableListItem;
}(Component);

_defineProperty(EuiSelectableListItem, "defaultProps", {
  showIcons: true,
  onFocusBadge: true
});