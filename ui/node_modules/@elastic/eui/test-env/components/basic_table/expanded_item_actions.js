"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ExpandedItemActions = void 0;

var _react = _interopRequireDefault(require("react"));

var _classnames = _interopRequireDefault(require("classnames"));

var _default_item_action = require("./default_item_action");

var _custom_item_action = require("./custom_item_action");

var _action_types = require("./action_types");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var ExpandedItemActions = function ExpandedItemActions(_ref) {
  var actions = _ref.actions,
      itemId = _ref.itemId,
      item = _ref.item,
      actionEnabled = _ref.actionEnabled,
      className = _ref.className;
  var moreThanThree = actions.length > 2;
  return (0, _react2.jsx)(_react.default.Fragment, null, actions.reduce(function (tools, action, index) {
    var available = action.available ? action.available(item) : true;

    if (!available) {
      return tools;
    }

    var enabled = actionEnabled(action);
    var key = "item_action_".concat(itemId, "_").concat(index);
    var classes = (0, _classnames.default)(className, {
      expandedItemActions__completelyHide: moreThanThree && index < 2
    });

    if ((0, _action_types.isCustomItemAction)(action)) {
      // custom action has a render function
      tools.push((0, _react2.jsx)(_custom_item_action.CustomItemAction, {
        key: key,
        className: classes,
        index: index,
        action: action,
        enabled: enabled,
        item: item
      }));
    } else {
      tools.push((0, _react2.jsx)(_default_item_action.DefaultItemAction, {
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

exports.ExpandedItemActions = ExpandedItemActions;