import _extends from "@babel/runtime/helpers/extends";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import classNames from 'classnames';
import React from 'react';
import { EuiButtonDisplay } from '../button';
import { useInnerText } from '../../inner_text';
import { useGeneratedHtmlId } from '../../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiButtonGroupButton = function EuiButtonGroupButton(_ref) {
  var className = _ref.className,
      id = _ref.id,
      isDisabled = _ref.isDisabled,
      isIconOnly = _ref.isIconOnly,
      _ref$isSelected = _ref.isSelected,
      isSelected = _ref$isSelected === void 0 ? false : _ref$isSelected,
      label = _ref.label,
      name = _ref.name,
      _onChange = _ref.onChange,
      size = _ref.size,
      value = _ref.value,
      _ref$element = _ref.element,
      element = _ref$element === void 0 ? 'button' : _ref$element,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'button' : _ref$type,
      rest = _objectWithoutProperties(_ref, ["className", "id", "isDisabled", "isIconOnly", "isSelected", "label", "name", "onChange", "size", "value", "element", "type"]);

  // Force element to be a button if disabled
  var el = isDisabled ? 'button' : element;
  var newId = useGeneratedHtmlId();
  var elementProps = {};
  var singleInput;

  if (el === 'label') {
    elementProps = _objectSpread(_objectSpread({}, elementProps), {}, {
      htmlFor: newId
    });
    singleInput = ___EmotionJSX("input", {
      id: newId,
      className: "euiScreenReaderOnly",
      name: name,
      checked: isSelected,
      disabled: isDisabled,
      value: value,
      type: "radio",
      onChange: function onChange() {
        return _onChange(id, value);
      },
      "data-test-subj": id
    });
  } else {
    elementProps = _objectSpread(_objectSpread({}, elementProps), {}, {
      id: newId,
      'data-test-subj': id,
      isSelected: isSelected,
      type: type,
      onClick: function onClick() {
        return _onChange(id);
      }
    });
  }

  var buttonClasses = classNames({
    'euiButtonGroupButton-isSelected': isSelected,
    'euiButtonGroupButton-isIconOnly': isIconOnly
  }, className);
  /**
   * Because the selected buttons also increase their text weight to 'bold',
   * we don't want the whole button size to shift when selected, so we determine
   * the base width of the button via the `euiTextShift()` method in SASS.
   */

  var _useInnerText = useInnerText(),
      _useInnerText2 = _slicedToArray(_useInnerText, 2),
      buttonTextRef = _useInnerText2[0],
      innerText = _useInnerText2[1];

  return ___EmotionJSX(EuiButtonDisplay, _extends({
    baseClassName: "euiButtonGroupButton",
    className: buttonClasses,
    element: el,
    fill: size !== 'compressed' && isSelected,
    isDisabled: isDisabled,
    size: size === 'compressed' ? 's' : size,
    textProps: {
      className: isIconOnly ? 'euiScreenReaderOnly' : 'euiButtonGroupButton__textShift',
      ref: buttonTextRef,
      'data-text': innerText
    },
    title: innerText
  }, elementProps, rest), singleInput, label);
};