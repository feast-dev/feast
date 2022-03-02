import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import classNames from 'classnames';
import { EuiFormFieldset } from '../form_fieldset';
import { EuiCheckbox } from './checkbox';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiCheckboxGroup = function EuiCheckboxGroup(_ref) {
  var _ref$options = _ref.options,
      options = _ref$options === void 0 ? [] : _ref$options,
      _ref$idToSelectedMap = _ref.idToSelectedMap,
      idToSelectedMap = _ref$idToSelectedMap === void 0 ? {} : _ref$idToSelectedMap,
      onChange = _ref.onChange,
      className = _ref.className,
      disabled = _ref.disabled,
      compressed = _ref.compressed,
      legend = _ref.legend,
      rest = _objectWithoutProperties(_ref, ["options", "idToSelectedMap", "onChange", "className", "disabled", "compressed", "legend"]);

  var checkboxes = options.map(function (option, index) {
    var isOptionDisabled = option.disabled,
        optionClass = option.className,
        optionRest = _objectWithoutProperties(option, ["disabled", "className"]);

    return ___EmotionJSX(EuiCheckbox, _extends({
      className: classNames('euiCheckboxGroup__item', optionClass),
      key: index,
      checked: idToSelectedMap[option.id],
      disabled: disabled || isOptionDisabled,
      onChange: onChange.bind(null, option.id),
      compressed: compressed
    }, optionRest));
  });

  if (!!legend) {
    // Be sure to pass down the compressed option to the legend
    legend.compressed = compressed;
    return ___EmotionJSX(EuiFormFieldset, _extends({
      className: className,
      legend: legend
    }, rest), checkboxes);
  }

  return ___EmotionJSX("div", _extends({
    className: className
  }, rest), checkboxes);
};