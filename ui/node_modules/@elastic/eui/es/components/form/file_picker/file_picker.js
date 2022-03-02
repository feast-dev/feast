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
import { keysOf } from '../../common';
import { EuiValidatableControl } from '../validatable_control';
import { EuiButtonEmpty } from '../../button';
import { EuiProgress } from '../../progress';
import { EuiIcon } from '../../icon';
import { EuiI18n } from '../../i18n';
import { EuiLoadingSpinner } from '../../loading';
import { htmlIdGenerator } from '../../../services/accessibility';
import { jsx as ___EmotionJSX } from "@emotion/react";
var displayToClassNameMap = {
  default: null,
  large: 'euiFilePicker--large'
};
export var DISPLAYS = keysOf(displayToClassNameMap);
export var EuiFilePicker = /*#__PURE__*/function (_Component) {
  _inherits(EuiFilePicker, _Component);

  var _super = _createSuper(EuiFilePicker);

  function EuiFilePicker() {
    var _this;

    _classCallCheck(this, EuiFilePicker);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "state", {
      promptText: null,
      isHoveringDrop: false
    });

    _defineProperty(_assertThisInitialized(_this), "fileInput", null);

    _defineProperty(_assertThisInitialized(_this), "generatedId", htmlIdGenerator()());

    _defineProperty(_assertThisInitialized(_this), "handleChange", function () {
      if (!_this.fileInput) return;

      if (_this.fileInput.files && _this.fileInput.files.length > 1) {
        _this.setState({
          promptText: ___EmotionJSX(EuiI18n, {
            token: "euiFilePicker.filesSelected",
            default: "{fileCount} files selected",
            values: {
              fileCount: _this.fileInput.files.length
            }
          })
        });
      } else if (_this.fileInput.files && _this.fileInput.files.length === 0) {
        _this.setState({
          promptText: null
        });
      } else {
        _this.setState({
          promptText: _this.fileInput.value.split('\\').pop()
        });
      }

      var onChange = _this.props.onChange;

      if (onChange) {
        onChange(_this.fileInput.files);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "removeFiles", function (e) {
      if (e) {
        e.stopPropagation();
        e.preventDefault();
      }

      if (!_this.fileInput) return;
      _this.fileInput.value = '';

      _this.handleChange();
    });

    _defineProperty(_assertThisInitialized(_this), "showDrop", function () {
      if (!_this.props.disabled) {
        _this.setState({
          isHoveringDrop: true
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "hideDrop", function () {
      _this.setState({
        isHoveringDrop: false
      });
    });

    return _this;
  }

  _createClass(EuiFilePicker, [{
    key: "render",
    value: function render() {
      var _this2 = this;

      return ___EmotionJSX(EuiI18n, {
        token: "euiFilePicker.clearSelectedFiles",
        default: "Clear selected files"
      }, function (clearSelectedFiles) {
        var _this2$props = _this2.props,
            id = _this2$props.id,
            name = _this2$props.name,
            initialPromptText = _this2$props.initialPromptText,
            className = _this2$props.className,
            disabled = _this2$props.disabled,
            compressed = _this2$props.compressed,
            onChange = _this2$props.onChange,
            isInvalid = _this2$props.isInvalid,
            fullWidth = _this2$props.fullWidth,
            isLoading = _this2$props.isLoading,
            display = _this2$props.display,
            rest = _objectWithoutProperties(_this2$props, ["id", "name", "initialPromptText", "className", "disabled", "compressed", "onChange", "isInvalid", "fullWidth", "isLoading", "display"]);

        var promptId = "".concat(id || _this2.generatedId, "-filePicker__prompt");
        var isOverridingInitialPrompt = _this2.state.promptText != null;
        var normalFormControl = display === 'default';
        var classes = classNames('euiFilePicker', displayToClassNameMap[display], {
          euiFilePicker__showDrop: _this2.state.isHoveringDrop,
          'euiFilePicker--compressed': compressed,
          'euiFilePicker--fullWidth': fullWidth,
          'euiFilePicker-isInvalid': isInvalid,
          'euiFilePicker-isLoading': isLoading,
          'euiFilePicker-hasFiles': isOverridingInitialPrompt
        }, className);
        var clearButton;

        if (isLoading && normalFormControl) {
          // Override clear button with loading spinner if it is in loading state
          clearButton = ___EmotionJSX(EuiLoadingSpinner, {
            className: "euiFilePicker__loadingSpinner"
          });
        } else if (isOverridingInitialPrompt) {
          if (normalFormControl) {
            clearButton = ___EmotionJSX("button", {
              type: "button",
              "aria-label": clearSelectedFiles,
              className: "euiFilePicker__clearButton",
              onClick: _this2.removeFiles
            }, ___EmotionJSX(EuiIcon, {
              className: "euiFilePicker__clearIcon",
              type: "cross"
            }));
          } else {
            clearButton = ___EmotionJSX(EuiButtonEmpty, {
              "aria-label": clearSelectedFiles,
              className: "euiFilePicker__clearButton",
              size: "xs",
              onClick: _this2.removeFiles
            }, ___EmotionJSX(EuiI18n, {
              token: "euiFilePicker.removeSelected",
              default: "Remove"
            }));
          }
        } else {
          clearButton = null;
        }

        var loader = !normalFormControl && isLoading && ___EmotionJSX(EuiProgress, {
          size: "xs",
          color: "accent",
          position: "absolute"
        });

        return ___EmotionJSX("div", {
          className: classes
        }, ___EmotionJSX("div", {
          className: "euiFilePicker__wrap"
        }, ___EmotionJSX(EuiValidatableControl, {
          isInvalid: isInvalid
        }, ___EmotionJSX("input", _extends({
          type: "file",
          id: id,
          name: name,
          className: "euiFilePicker__input",
          onChange: _this2.handleChange,
          ref: function ref(input) {
            _this2.fileInput = input;
          },
          onDragOver: _this2.showDrop,
          onDragLeave: _this2.hideDrop,
          onDrop: _this2.hideDrop,
          disabled: disabled,
          "aria-describedby": promptId
        }, rest))), ___EmotionJSX("div", {
          className: "euiFilePicker__prompt",
          id: promptId
        }, ___EmotionJSX(EuiIcon, {
          className: "euiFilePicker__icon",
          type: "importAction",
          size: normalFormControl ? 'm' : 'l',
          "aria-hidden": "true"
        }), ___EmotionJSX("div", {
          className: "euiFilePicker__promptText"
        }, _this2.state.promptText || initialPromptText), clearButton, loader)));
      });
    }
  }]);

  return EuiFilePicker;
}(Component);

_defineProperty(EuiFilePicker, "defaultProps", {
  initialPromptText: ___EmotionJSX(EuiI18n, {
    token: "euiFilePicker.promptText",
    default: "Select or drag and drop a file"
  }),
  compressed: false,
  display: 'large'
});

EuiFilePicker.propTypes = {
  id: PropTypes.string,
  name: PropTypes.string,
  className: PropTypes.string,

  /**
     * The content that appears in the dropzone if no file is attached
     */
  initialPromptText: PropTypes.node,

  /**
     * Use as a callback to access the HTML FileList API
     */
  onChange: PropTypes.func,

  /**
     * Reduces the size to a typical (compressed) input
     */
  compressed: PropTypes.bool,

  /**
     * Size or type of display;
     * `default` for normal height, similar to other controls;
     * `large` for taller size
     */
  display: PropTypes.oneOf(["default", "large"]),
  fullWidth: PropTypes.bool,
  isInvalid: PropTypes.bool,
  isLoading: PropTypes.bool,
  disabled: PropTypes.bool,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};