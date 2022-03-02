function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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
import classNames from 'classnames';
import PropTypes from "prop-types";
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

EuiControlBar.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Show or hide the content area containing the `children`
       */
  showContent: PropTypes.bool,

  /**
       * An array of controls, actions, and layout spacers to display.
       * Accepts `'button' | 'tab' | 'breadcrumbs' | 'text' | 'icon' | 'spacer' | 'divider'`
       */
  controls: PropTypes.arrayOf(PropTypes.shape({
    href: PropTypes.string,
    onClick: PropTypes.func,
    id: PropTypes.string,
    label: PropTypes.oneOfType([PropTypes.node.isRequired, PropTypes.node]),
    buttonRef: PropTypes.any,
    controlType: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.oneOf(["button"]).isRequired, PropTypes.oneOfType([PropTypes.oneOf(["breadcrumbs"]).isRequired, PropTypes.oneOf(["tab"]).isRequired]).isRequired]).isRequired, PropTypes.oneOf(["text"]).isRequired]).isRequired, PropTypes.oneOf(["icon"]).isRequired]).isRequired, PropTypes.oneOf(["divider"]).isRequired]).isRequired, PropTypes.oneOf(["spacer"]).isRequired]).isRequired,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,

    /**
       * Hides extra (above the max) breadcrumbs under a collapsed item as the window gets smaller.
       * Pass a custom #EuiBreadcrumbResponsiveMaxCount object to change the number of breadcrumbs to show at the particular breakpoints.
       *
       * Pass `false` to turn this behavior off.
       *
       * Default: `{ xs: 1, s: 2, m: 4 }`
       */
    responsive: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.any.isRequired]),

    /**
       * Forces all breadcrumbs to single line and
       * truncates each breadcrumb to a particular width,
       * except for the last item
       */
    truncate: PropTypes.bool,

    /**
       * Collapses the inner items past the maximum set here
       * into a single ellipses item.
       * Omitting or passing a `0` value will show all breadcrumbs.
       */
    max: PropTypes.oneOfType([PropTypes.number.isRequired, PropTypes.oneOf([null])]),

    /**
       * The array of individual #EuiBreadcrumb items
       */
    breadcrumbs: PropTypes.arrayOf(PropTypes.shape({
      className: PropTypes.string,
      "aria-label": PropTypes.string,
      "data-test-subj": PropTypes.string,

      /**
         * Visible label of the breadcrumb
         */
      text: PropTypes.node.isRequired,
      href: PropTypes.string,
      onClick: PropTypes.func,

      /**
         * Force a max-width on the breadcrumb text
         */
      truncate: PropTypes.bool,

      /**
         * Override the existing `aria-current` which defaults to `page` for the last breadcrumb
         */
      "aria-current": PropTypes.any
    }).isRequired),
    text: PropTypes.node,
    iconType: PropTypes.string
  }).isRequired).isRequired,

  /**
       * The default height of the content area.
       */
  size: PropTypes.oneOf(["s", "m", "l"]),

  /**
       * Customize the max height.
       * Best when used with `size=l` as this will ensure the actual height equals the max height set.
       */
  maxHeight: PropTypes.oneOfType([PropTypes.number.isRequired, PropTypes.string.isRequired]),

  /**
       * Set the offset from the left side of the screen.
       */
  leftOffset: PropTypes.oneOfType([PropTypes.number.isRequired, PropTypes.string.isRequired]),

  /**
       * Set the offset from the left side of the screen.
       */
  rightOffset: PropTypes.oneOfType([PropTypes.number.isRequired, PropTypes.string.isRequired]),

  /**
       * The control bar is hidden on mobile by default. Use the `showOnMobile` prop to force it's display on mobile screens.
       * You'll need to ensure that the content you place into the bar renders as expected on mobile.
       */
  showOnMobile: PropTypes.bool,

  /**
       * By default EuiControlBar will live in a portal, fixed position to the browser window.
       * Change the position of the bar to live inside a container and be positioned against its parent.
       */
  position: PropTypes.oneOf(["fixed", "relative", "absolute"]),

  /**
       * Optional class applied to the body used when `position = fixed`
       */
  bodyClassName: PropTypes.string,

  /**
       * Customize the screen reader heading that helps users find this control. Default is "Page level controls".
       */
  landmarkHeading: PropTypes.string
};