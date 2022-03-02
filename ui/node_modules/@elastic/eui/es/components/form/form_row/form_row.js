function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _toConsumableArray(arr) { return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _unsupportedIterableToArray(arr) || _nonIterableSpread(); }

function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _iterableToArray(iter) { if (typeof Symbol !== "undefined" && Symbol.iterator in Object(iter)) return Array.from(iter); }

function _arrayWithoutHoles(arr) { if (Array.isArray(arr)) return _arrayLikeToArray(arr); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

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
import React, { cloneElement, Component, Children } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { keysOf } from '../../common';
import { get } from '../../../services/objects';
import { EuiFormHelpText } from '../form_help_text';
import { EuiFormErrorText } from '../form_error_text';
import { EuiFormLabel } from '../form_label';
import { htmlIdGenerator } from '../../../services/accessibility';
import { jsx as ___EmotionJSX } from "@emotion/react";
var displayToClassNameMap = {
  row: null,
  rowCompressed: 'euiFormRow--compressed',
  columnCompressed: 'euiFormRow--compressed euiFormRow--horizontal',
  center: null,
  centerCompressed: 'euiFormRow--compressed',
  columnCompressedSwitch: 'euiFormRow--compressed euiFormRow--horizontal euiFormRow--hasSwitch'
};
export var DISPLAYS = keysOf(displayToClassNameMap);
export var EuiFormRow = /*#__PURE__*/function (_Component) {
  _inherits(EuiFormRow, _Component);

  var _super = _createSuper(EuiFormRow);

  function EuiFormRow() {
    var _this;

    _classCallCheck(this, EuiFormRow);

    for (var _len = arguments.length, _args = new Array(_len), _key = 0; _key < _len; _key++) {
      _args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(_args));

    _defineProperty(_assertThisInitialized(_this), "state", {
      isFocused: false,
      id: _this.props.id || htmlIdGenerator()()
    });

    _defineProperty(_assertThisInitialized(_this), "onFocus", function () {
      // Doing this to allow onFocus to be called correctly from the child input element as this component overrides it
      var onChildFocus = get(_this.props, 'children.props.onFocus');

      if (onChildFocus) {
        onChildFocus.apply(void 0, arguments);
      }

      _this.setState(function (_ref) {
        var isFocused = _ref.isFocused;

        if (!isFocused) {
          return {
            isFocused: true
          };
        } else {
          return null;
        }
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onBlur", function () {
      // Doing this to allow onBlur to be called correctly from the child input element as this component overrides it
      var onChildBlur = get(_this.props, 'children.props.onBlur');

      if (onChildBlur) {
        onChildBlur.apply(void 0, arguments);
      }

      _this.setState({
        isFocused: false
      });
    });

    return _this;
  }

  _createClass(EuiFormRow, [{
    key: "render",
    value: function render() {
      var _ref2, _child$props$disabled;

      var _this$props = this.props,
          children = _this$props.children,
          helpText = _this$props.helpText,
          isInvalid = _this$props.isInvalid,
          error = _this$props.error,
          label = _this$props.label,
          labelType = _this$props.labelType,
          labelAppend = _this$props.labelAppend,
          hasEmptyLabelSpace = _this$props.hasEmptyLabelSpace,
          fullWidth = _this$props.fullWidth,
          className = _this$props.className,
          describedByIds = _this$props.describedByIds,
          display = _this$props.display,
          hasChildLabel = _this$props.hasChildLabel,
          propsId = _this$props.id,
          isDisabled = _this$props.isDisabled,
          rest = _objectWithoutProperties(_this$props, ["children", "helpText", "isInvalid", "error", "label", "labelType", "labelAppend", "hasEmptyLabelSpace", "fullWidth", "className", "describedByIds", "display", "hasChildLabel", "id", "isDisabled"]);

      var id = this.state.id;
      var classes = classNames('euiFormRow', {
        'euiFormRow--hasEmptyLabelSpace': hasEmptyLabelSpace,
        'euiFormRow--fullWidth': fullWidth
      }, displayToClassNameMap[display], // Safe use of ! as default prop is 'row'
      className);
      var optionalHelpTexts;

      if (helpText) {
        var helpTexts = Array.isArray(helpText) ? helpText : [helpText];
        optionalHelpTexts = helpTexts.map(function (helpText, i) {
          var key = typeof helpText === 'string' ? helpText : i;
          return ___EmotionJSX(EuiFormHelpText, {
            key: key,
            id: "".concat(id, "-help-").concat(i),
            className: "euiFormRow__text"
          }, helpText);
        });
      }

      var optionalErrors;

      if (error && isInvalid) {
        var errorTexts = Array.isArray(error) ? error : [error];
        optionalErrors = errorTexts.map(function (error, i) {
          var key = typeof error === 'string' ? error : i;
          return ___EmotionJSX(EuiFormErrorText, {
            key: key,
            id: "".concat(id, "-error-").concat(i),
            className: "euiFormRow__text"
          }, error);
        });
      }

      var optionalLabel;
      var isLegend = label && labelType === 'legend' ? true : false;

      if (label || labelAppend) {
        var labelProps = {};

        if (isLegend) {
          labelProps = {
            type: labelType
          };
        } else {
          labelProps = _objectSpread(_objectSpread({
            htmlFor: hasChildLabel ? id : undefined
          }, !isDisabled && {
            isFocused: this.state.isFocused
          }), {}, {
            // If the row is disabled, don't pass the isFocused state.
            type: labelType
          });
        }

        optionalLabel = ___EmotionJSX("div", {
          className: "euiFormRow__labelWrapper"
        }, ___EmotionJSX(EuiFormLabel, _extends({
          className: "euiFormRow__label",
          isInvalid: isInvalid,
          isDisabled: isDisabled,
          "aria-invalid": isInvalid
        }, labelProps), label), labelAppend && ' ', labelAppend);
      }

      var optionalProps = {};
      /**
       * Safe use of ! as default prop is []
       */

      var describingIds = _toConsumableArray(describedByIds);

      if (optionalHelpTexts) {
        optionalHelpTexts.forEach(function (optionalHelpText) {
          return describingIds.push(optionalHelpText.props.id);
        });
      }

      if (optionalErrors) {
        optionalErrors.forEach(function (error) {
          return describingIds.push(error.props.id);
        });
      }

      if (describingIds.length > 0) {
        optionalProps['aria-describedby'] = describingIds.join(' ');
      }

      var child = Children.only(children);
      var field = /*#__PURE__*/cloneElement(child, _objectSpread({
        id: id,
        // Allow the child's disabled or isDisabled prop to supercede the `isDisabled`
        disabled: (_ref2 = (_child$props$disabled = child.props.disabled) !== null && _child$props$disabled !== void 0 ? _child$props$disabled : child.props.isDisabled) !== null && _ref2 !== void 0 ? _ref2 : isDisabled,
        onFocus: this.onFocus,
        onBlur: this.onBlur
      }, optionalProps));
      var fieldWrapperClasses = classNames('euiFormRow__fieldWrapper', {
        euiFormRow__fieldWrapperDisplayOnly:
        /**
         * Safe use of ! as default prop is 'row'
         */
        display.startsWith('center')
      });
      var sharedProps = {
        className: classes,
        id: "".concat(id, "-row")
      };

      var contents = ___EmotionJSX(React.Fragment, null, optionalLabel, ___EmotionJSX("div", {
        className: fieldWrapperClasses
      }, field, optionalErrors, optionalHelpTexts));

      return labelType === 'legend' ? ___EmotionJSX("fieldset", _extends({}, sharedProps, rest), contents) : ___EmotionJSX("div", _extends({}, sharedProps, rest), contents);
    }
  }]);

  return EuiFormRow;
}(Component);

_defineProperty(EuiFormRow, "defaultProps", {
  display: 'row',
  hasEmptyLabelSpace: false,
  fullWidth: false,
  describedByIds: [],
  labelType: 'label',
  hasChildLabel: true
});

EuiFormRow.propTypes = {
  /**
     * Defaults to rendering a `<label>` but if passed `'legend'` for labelType,
     * will render both a `<legend>` and the surrounding container as a `<fieldset>`
     */
  labelType: PropTypes.oneOfType([PropTypes.oneOf(["label"]), PropTypes.oneOf(["legend"])]),
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * When `rowCompressed`, just tightens up the spacing;
     * Set to `columnCompressed` if compressed
     * and horizontal layout is needed.
     * Set to `center` or `centerCompressed` to align non-input
     * content better with inline rows.
     * Set to `columnCompressedSwitch` if the form control being passed
     * as the child is a switch.
     */

  /**
     * When `rowCompressed`, just tightens up the spacing;
     * Set to `columnCompressed` if compressed
     * and horizontal layout is needed.
     * Set to `center` or `centerCompressed` to align non-input
     * content better with inline rows.
     * Set to `columnCompressedSwitch` if the form control being passed
     * as the child is a switch.
     */
  display: PropTypes.oneOf(["row", "rowCompressed", "columnCompressed", "center", "centerCompressed", "columnCompressedSwitch"]),
  hasEmptyLabelSpace: PropTypes.bool,
  fullWidth: PropTypes.bool,

  /**
     * IDs of additional elements that should be part of children's `aria-describedby`
     */

  /**
     * IDs of additional elements that should be part of children's `aria-describedby`
     */
  describedByIds: PropTypes.arrayOf(PropTypes.string.isRequired),

  /**
     * Escape hatch to not render duplicate labels if the child also renders a label
     */

  /**
     * Escape hatch to not render duplicate labels if the child also renders a label
     */
  hasChildLabel: PropTypes.bool,

  /**
     * ReactElement to render as this component's content
     */

  /**
     * ReactElement to render as this component's content
     */
  children: PropTypes.element.isRequired,
  label: PropTypes.node,

  /**
     * Adds an extra node to the right of the form label without
     * being contained inside the form label. Good for things
     * like documentation links.
     */

  /**
     * Adds an extra node to the right of the form label without
     * being contained inside the form label. Good for things
     * like documentation links.
     */
  labelAppend: PropTypes.any,
  id: PropTypes.string,
  isInvalid: PropTypes.bool,
  error: PropTypes.oneOfType([PropTypes.node.isRequired, PropTypes.arrayOf(PropTypes.node.isRequired).isRequired]),

  /**
     *  Adds a single node/string or an array of nodes/strings below the input
     */

  /**
     *  Adds a single node/string or an array of nodes/strings below the input
     */
  helpText: PropTypes.oneOfType([PropTypes.node.isRequired, PropTypes.arrayOf(PropTypes.node.isRequired).isRequired]),

  /**
     *  Passed along to the label element; and to the child field element when `disabled` doesn't already exist on the child field element.
     */

  /**
     *  Passed along to the label element; and to the child field element when `disabled` doesn't already exist on the child field element.
     */
  isDisabled: PropTypes.bool
};