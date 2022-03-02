import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import classNames from 'classnames';
import React from 'react';
import { EuiScreenReaderOnly } from '../../accessibility';
import { EuiButtonGroupButton } from './button_group_button';
import { colorToClassNameMap } from '../button';
import { useGeneratedHtmlId } from '../../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";
var groupSizeToClassNameMap = {
  s: '--small',
  m: '--medium',
  compressed: '--compressed'
};
export var EuiButtonGroup = function EuiButtonGroup(_ref) {
  var className = _ref.className,
      _ref$buttonSize = _ref.buttonSize,
      buttonSize = _ref$buttonSize === void 0 ? 's' : _ref$buttonSize,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'text' : _ref$color,
      _ref$idSelected = _ref.idSelected,
      idSelected = _ref$idSelected === void 0 ? '' : _ref$idSelected,
      _ref$idToSelectedMap = _ref.idToSelectedMap,
      idToSelectedMap = _ref$idToSelectedMap === void 0 ? {} : _ref$idToSelectedMap,
      _ref$isDisabled = _ref.isDisabled,
      isDisabled = _ref$isDisabled === void 0 ? false : _ref$isDisabled,
      _ref$isFullWidth = _ref.isFullWidth,
      isFullWidth = _ref$isFullWidth === void 0 ? false : _ref$isFullWidth,
      _ref$isIconOnly = _ref.isIconOnly,
      isIconOnly = _ref$isIconOnly === void 0 ? false : _ref$isIconOnly,
      legend = _ref.legend,
      name = _ref.name,
      onChange = _ref.onChange,
      _ref$options = _ref.options,
      options = _ref$options === void 0 ? [] : _ref$options,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'single' : _ref$type,
      rest = _objectWithoutProperties(_ref, ["className", "buttonSize", "color", "idSelected", "idToSelectedMap", "isDisabled", "isFullWidth", "isIconOnly", "legend", "name", "onChange", "options", "type"]);

  // Compressed style can't support `ghost` color because it's more like a form field than a button
  var badColorCombo = buttonSize === 'compressed' && color === 'ghost';
  var resolvedColor = badColorCombo ? 'text' : color;

  if (badColorCombo) {
    console.warn('EuiButtonGroup of compressed size does not support the ghost color. It will render as text instead.');
  }

  var classes = classNames('euiButtonGroup', "euiButtonGroup".concat(groupSizeToClassNameMap[buttonSize]), "euiButtonGroup".concat(colorToClassNameMap[resolvedColor]), {
    'euiButtonGroup--fullWidth': isFullWidth,
    'euiButtonGroup--isDisabled': isDisabled
  }, className);
  var typeIsSingle = type === 'single';
  var nameIfSingle = useGeneratedHtmlId({
    conditionalId: name
  });
  return ___EmotionJSX("fieldset", _extends({
    className: classes
  }, rest, {
    disabled: isDisabled
  }), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("legend", null, legend)), ___EmotionJSX("div", {
    className: "euiButtonGroup__buttons"
  }, options.map(function (option, index) {
    return ___EmotionJSX(EuiButtonGroupButton, _extends({
      key: index,
      name: nameIfSingle,
      isDisabled: isDisabled
    }, option, {
      element: typeIsSingle ? 'label' : 'button',
      isSelected: typeIsSingle ? option.id === idSelected : idToSelectedMap[option.id],
      color: resolvedColor,
      size: buttonSize,
      isIconOnly: isIconOnly,
      onChange: onChange
    }));
  })));
};