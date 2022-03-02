import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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
import { EuiScreenReaderOnly } from '../accessibility';
import { EuiBreadcrumbs } from '../breadcrumbs';
import { EuiButton, EuiButtonIcon } from '../button';
import { EuiI18n } from '../i18n';
import { EuiIcon } from '../icon';
import { EuiPortal } from '../portal';
/**
 * Extends EuiButton excluding `size`. Requires `label` as the `children`.
 */

import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiControlBar = /*#__PURE__*/function (_Component) {
  _inherits(EuiControlBar, _Component);

  var _super = _createSuper(EuiControlBar);

  function EuiControlBar() {
    var _this;

    _classCallCheck(this, EuiControlBar);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "bar", null);

    _defineProperty(_assertThisInitialized(_this), "state", {
      selectedTab: ''
    });

    return _this;
  }

  _createClass(EuiControlBar, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      if (this.props.position === 'fixed') {
        var height = this.bar ? this.bar.clientHeight : -1;
        document.body.style.paddingBottom = "".concat(height, "px");

        if (this.props.bodyClassName) {
          document.body.classList.add(this.props.bodyClassName);
        }
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      document.body.style.paddingBottom = '';

      if (this.props.bodyClassName) {
        document.body.classList.remove(this.props.bodyClassName);
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props = this.props,
          children = _this$props.children,
          className = _this$props.className,
          showContent = _this$props.showContent,
          controls = _this$props.controls,
          size = _this$props.size,
          leftOffset = _this$props.leftOffset,
          rightOffset = _this$props.rightOffset,
          maxHeight = _this$props.maxHeight,
          showOnMobile = _this$props.showOnMobile,
          style = _this$props.style,
          position = _this$props.position,
          bodyClassName = _this$props.bodyClassName,
          landmarkHeading = _this$props.landmarkHeading,
          rest = _objectWithoutProperties(_this$props, ["children", "className", "showContent", "controls", "size", "leftOffset", "rightOffset", "maxHeight", "showOnMobile", "style", "position", "bodyClassName", "landmarkHeading"]);

      var styles = _objectSpread(_objectSpread({}, style), {}, {
        left: leftOffset,
        right: rightOffset,
        maxHeight: maxHeight
      });

      var classes = classNames('euiControlBar', className, {
        'euiControlBar-isOpen': showContent,
        'euiControlBar--large': size === 'l',
        'euiControlBar--medium': size === 'm',
        'euiControlBar--small': size === 's',
        'euiControlBar--fixed': position === 'fixed',
        'euiControlBar--absolute': position === 'absolute',
        'euiControlBar--relative': position === 'relative',
        'euiControlBar--showOnMobile': showOnMobile
      });

      var handleTabClick = function handleTabClick(control, e) {
        _this2.setState({
          selectedTab: control.id
        }, function () {
          control.onClick(e);
        });
      };

      var controlItem = function controlItem(control, index) {
        switch (control.controlType) {
          case 'button':
            {
              var controlType = control.controlType,
                  id = control.id,
                  _control$color = control.color,
                  color = _control$color === void 0 ? 'ghost' : _control$color,
                  label = control.label,
                  _className = control.className,
                  _rest = _objectWithoutProperties(control, ["controlType", "id", "color", "label", "className"]);

              return ___EmotionJSX(EuiButton, _extends({
                key: id + index,
                className: classNames('euiControlBar__button', _className),
                color: color
              }, _rest, {
                size: "s"
              }), label);
            }

          case 'icon':
            {
              var _controlType = control.controlType,
                  _id = control.id,
                  iconType = control.iconType,
                  _className2 = control.className,
                  _control$color2 = control.color,
                  _color = _control$color2 === void 0 ? 'ghost' : _control$color2,
                  onClick = control.onClick,
                  href = control.href,
                  _rest2 = _objectWithoutProperties(control, ["controlType", "id", "iconType", "className", "color", "onClick", "href"]);

              return onClick || href ? ___EmotionJSX(EuiButtonIcon, _extends({
                key: _id + index,
                className: classNames('euiControlBar__buttonIcon', _className2),
                onClick: onClick,
                href: href,
                color: _color
              }, _rest2, {
                iconType: iconType
              })) : ___EmotionJSX(EuiIcon, _extends({
                key: _id + index,
                className: classNames('euiControlBar__icon', _className2),
                type: iconType,
                color: _color
              }, _rest2));
            }

          case 'divider':
            return ___EmotionJSX("div", {
              key: control.controlType + index,
              className: "euiControlBar__divider"
            });

          case 'spacer':
            return ___EmotionJSX("div", {
              key: control.controlType + index,
              className: "euiControlBar__spacer"
            });

          case 'text':
            {
              var _controlType2 = control.controlType,
                  _id2 = control.id,
                  text = control.text,
                  _className3 = control.className,
                  _rest3 = _objectWithoutProperties(control, ["controlType", "id", "text", "className"]);

              return ___EmotionJSX("div", _extends({
                key: _id2,
                className: classNames('euiControlBar__text', _className3)
              }, _rest3), text);
            }

          case 'tab':
            {
              var _controlType3 = control.controlType,
                  _id3 = control.id,
                  _label = control.label,
                  _onClick = control.onClick,
                  _className4 = control.className,
                  _rest4 = _objectWithoutProperties(control, ["controlType", "id", "label", "onClick", "className"]);

              var tabClasses = classNames('euiControlBar__tab', {
                'euiControlBar__tab--active': showContent && _id3 === _this2.state.selectedTab
              }, _className4);
              return ___EmotionJSX("button", _extends({
                key: _id3 + index,
                className: tabClasses,
                onClick: function onClick(event) {
                  return handleTabClick(control, event);
                }
              }, _rest4), _label);
            }

          case 'breadcrumbs':
            {
              var _controlType4 = control.controlType,
                  _id4 = control.id,
                  _rest5 = _objectWithoutProperties(control, ["controlType", "id"]);

              return ___EmotionJSX(EuiBreadcrumbs, _extends({
                className: "euiControlBar__breadcrumbs",
                key: control.id
              }, _rest5));
            }
        }
      };

      var controlBar = ___EmotionJSX(EuiI18n, {
        token: "euiControlBar.screenReaderHeading",
        default: "Page level controls"
      }, function (screenReaderHeading) {
        return (// Though it would be better to use aria-labelledby than aria-label and not repeat the same string twice
          // A bug in voiceover won't list some landmarks in the rotor without an aria-label
          ___EmotionJSX("section", _extends({
            className: classes,
            "aria-label": landmarkHeading ? landmarkHeading : screenReaderHeading
          }, rest, {
            style: styles
          }), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("h2", null, landmarkHeading ? landmarkHeading : screenReaderHeading)), ___EmotionJSX("div", {
            className: "euiControlBar__controls",
            ref: function ref(node) {
              _this2.bar = node;
            }
          }, controls.map(function (control, index) {
            return controlItem(control, index);
          })), _this2.props.showContent ? ___EmotionJSX("div", {
            className: "euiControlBar__content"
          }, children) : null)
        );
      });

      return position === 'fixed' ? ___EmotionJSX(EuiPortal, null, controlBar, ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", {
        "aria-live": "assertive"
      }, landmarkHeading ? ___EmotionJSX(EuiI18n, {
        token: "euiControlBar.customScreenReaderAnnouncement",
        default: "There is a new region landmark called {landmarkHeading} with page level controls at the end of the document.",
        values: {
          landmarkHeading: landmarkHeading
        }
      }) : ___EmotionJSX(EuiI18n, {
        token: "euiControlBar.screenReaderAnnouncement",
        default: "There is a new region landmark with page level controls at the end of the document."
      })))) : controlBar;
    }
  }]);

  return EuiControlBar;
}(Component);

_defineProperty(EuiControlBar, "defaultProps", {
  leftOffset: 0,
  rightOffset: 0,
  position: 'fixed',
  size: 'l',
  showContent: false,
  showOnMobile: false
});