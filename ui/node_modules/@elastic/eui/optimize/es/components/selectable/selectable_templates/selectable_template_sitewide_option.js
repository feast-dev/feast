import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import classNames from 'classnames';
import { EuiIcon } from '../../../components/icon';
import { EuiAvatar } from '../../../components/avatar/avatar';
import { EuiHighlight } from '../../../components/highlight';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var euiSelectableTemplateSitewideFormatOptions = function euiSelectableTemplateSitewideFormatOptions(options) {
  return options.map(function (item) {
    var title = item.label;

    if (item.meta && item.meta.length) {
      title += " \u2022".concat(renderOptionMeta(item.meta, '', true));
    }

    return _objectSpread(_objectSpread({
      key: item.label,
      title: title
    }, item), {}, {
      className: classNames('euiSelectableTemplateSitewide__listItem', item.className),
      prepend: item.icon ? ___EmotionJSX(EuiIcon, _extends({
        color: "subdued",
        size: "l"
      }, item.icon)) : item.prepend,
      append: item.avatar ? ___EmotionJSX(EuiAvatar, _extends({
        type: "space",
        size: "s"
      }, item.avatar)) : item.append
    });
  });
};
export var euiSelectableTemplateSitewideRenderOptions = function euiSelectableTemplateSitewideRenderOptions(option, searchValue) {
  return ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiHighlight, {
    className: "euiSelectableTemplateSitewide__listItemTitle",
    search: searchValue
  }, option.label), renderOptionMeta(option.meta, searchValue));
};

function renderOptionMeta(meta) {
  var searchValue = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : '';
  var stringsOnly = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;
  if (!meta || meta.length < 1) return;
  var metas = meta.map(function (meta) {
    var text = meta.text,
        highlightSearchString = meta.highlightSearchString,
        className = meta.className,
        rest = _objectWithoutProperties(meta, ["text", "highlightSearchString", "className"]);

    if (stringsOnly) {
      return " ".concat(text);
    } // Start with the base and custom classes


    var metaClasses = classNames('euiSelectableTemplateSitewide__optionMeta', className); // If they provided a type, create the class and append

    if (meta.type) {
      metaClasses = classNames(["euiSelectableTemplateSitewide__optionMeta--".concat(meta.type)], metaClasses);
    }

    return ___EmotionJSX(EuiHighlight, _extends({
      search: highlightSearchString ? searchValue : '',
      className: metaClasses,
      key: text
    }, rest), text);
  });
  return stringsOnly ? metas : ___EmotionJSX("span", {
    className: "euiSelectableTemplateSitewide__optionMetasList"
  }, metas);
}