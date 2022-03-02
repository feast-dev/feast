/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import classNames from 'classnames';
import { DefaultItemAction } from './default_item_action';
import { CustomItemAction } from './custom_item_action';
import { isCustomItemAction } from './action_types';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var ExpandedItemActions = function ExpandedItemActions(_ref) {
  var actions = _ref.actions,
      itemId = _ref.itemId,
      item = _ref.item,
      actionEnabled = _ref.actionEnabled,
      className = _ref.className;
  var moreThanThree = actions.length > 2;
  return ___EmotionJSX(React.Fragment, null, actions.reduce(function (tools, action, index) {
    var available = action.available ? action.available(item) : true;

    if (!available) {
      return tools;
    }

    var enabled = actionEnabled(action);
    var key = "item_action_".concat(itemId, "_").concat(index);
    var classes = classNames(className, {
      expandedItemActions__completelyHide: moreThanThree && index < 2
    });

    if (isCustomItemAction(action)) {
      // custom action has a render function
      tools.push(___EmotionJSX(CustomItemAction, {
        key: key,
        className: classes,
        index: index,
        action: action,
        enabled: enabled,
        item: item
      }));
    } else {
      tools.push(___EmotionJSX(DefaultItemAction, {
        key: key,
        className: classes,
        action: action,
        enabled: enabled,
        item: item
      }));
    }

    return tools;
  }, []));
};