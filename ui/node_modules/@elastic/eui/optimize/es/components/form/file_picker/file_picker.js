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