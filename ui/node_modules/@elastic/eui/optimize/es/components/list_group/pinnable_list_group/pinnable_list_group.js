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
import React from 'react';
import classNames from 'classnames';
import { EuiI18n } from '../../i18n';
import { EuiListGroup } from '../list_group';
import { jsx as ___EmotionJSX } from "@emotion/react";
var pinExtraAction = {
  color: 'primary',
  iconType: 'pinFilled',
  iconSize: 's',
  className: 'euiPinnableListGroup__itemExtraAction'
};
var pinnedExtraAction = {
  color: 'primary',
  iconType: 'pinFilled',
  iconSize: 's',
  className: 'euiPinnableListGroup__itemExtraAction euiPinnableListGroup__itemExtraAction-pinned',
  alwaysShow: true
};
export var EuiPinnableListGroup = function EuiPinnableListGroup(_ref) {
  var className = _ref.className,
      listItems = _ref.listItems,
      pinTitle = _ref.pinTitle,
      unpinTitle = _ref.unpinTitle,
      onPinClick = _ref.onPinClick,
      rest = _objectWithoutProperties(_ref, ["className", "listItems", "pinTitle", "unpinTitle", "onPinClick"]);

  var classes = classNames('euiPinnableListGroup', className); // Alter listItems object with extra props

  var getNewListItems = function getNewListItems(pinExtraActionLabel, pinnedExtraActionLabel) {
    return listItems.map(function (item) {
      var pinned = item.pinned,
          _item$pinnable = item.pinnable,
          pinnable = _item$pinnable === void 0 ? true : _item$pinnable,
          itemProps = _objectWithoutProperties(item, ["pinned", "pinnable"]); // Make some declarations of props for the nav implementation


      itemProps.className = classNames('euiPinnableListGroup__item', item.className); // Add the pinning action unless the item has it's own extra action

      if (onPinClick && !itemProps.extraAction && pinnable) {
        // Different displays for pinned vs unpinned
        if (pinned) {
          itemProps.extraAction = _objectSpread(_objectSpread({}, pinnedExtraAction), {}, {
            title: unpinTitle ? unpinTitle(item) : pinnedExtraActionLabel,
            'aria-label': unpinTitle ? unpinTitle(item) : pinnedExtraActionLabel
          });
        } else {
          itemProps.extraAction = _objectSpread(_objectSpread({}, pinExtraAction), {}, {
            title: pinTitle ? pinTitle(item) : pinExtraActionLabel,
            'aria-label': pinTitle ? pinTitle(item) : pinExtraActionLabel
          });
        } // Return the item on click


        itemProps.extraAction.onClick = function () {
          return onPinClick(item);
        };
      }

      return itemProps;
    });
  };

  return ___EmotionJSX(EuiI18n, {
    tokens: ['euiPinnableListGroup.pinExtraActionLabel', 'euiPinnableListGroup.pinnedExtraActionLabel'],
    defaults: ['Pin item', 'Unpin item']
  }, function (_ref2) {
    var _ref3 = _slicedToArray(_ref2, 2),
        pinExtraActionLabel = _ref3[0],
        pinnedExtraActionLabel = _ref3[1];

    var newListItems = getNewListItems(pinExtraActionLabel, pinnedExtraActionLabel);
    return ___EmotionJSX(EuiListGroup, _extends({
      className: classes,
      listItems: newListItems
    }, rest));
  });
};