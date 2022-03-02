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
import { EuiRadio } from './radio';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiRadioGroup = function EuiRadioGroup(_ref) {
  var _ref$options = _ref.options,
      options = _ref$options === void 0 ? [] : _ref$options,
      idSelected = _ref.idSelected,
      onChange = _ref.onChange,
      name = _ref.name,
      className = _ref.className,
      disabled = _ref.disabled,
      compressed = _ref.compressed,
      legend = _ref.legend,
      rest = _objectWithoutProperties(_ref, ["options", "idSelected", "onChange", "name", "className", "disabled", "compressed", "legend"]);

  var radios = options.map(function (option, index) {
    var isOptionDisabled = option.disabled,
        optionClass = option.className,
        id = option.id,
        label = option.label,
        optionRest = _objectWithoutProperties(option, ["disabled", "className", "id", "label"]);

    return ___EmotionJSX(EuiRadio, _extends({
      className: classNames('euiRadioGroup__item', optionClass),
      key: index,
      name: name,
      checked: id === idSelected,
      disabled: disabled || isOptionDisabled,
      onChange: onChange.bind(null, id, option.value),
      compressed: compressed,
      id: id,
      label: label
    }, optionRest));
  });

  if (!!legend) {
    // Be sure to pass down the compressed option to the legend
    legend.compressed = compressed;
    return ___EmotionJSX(EuiFormFieldset, _extends({
      className: className,
      legend: legend
    }, rest), radios);
  }

  return ___EmotionJSX("div", _extends({
    className: className
  }, rest), radios);
};