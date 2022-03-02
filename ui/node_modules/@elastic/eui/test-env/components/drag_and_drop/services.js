"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.euiDragDropCopy = exports.euiDragDropMove = exports.euiDragDropReorder = void 0;

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var euiDragDropReorder = function euiDragDropReorder(list, startIndex, endIndex) {
  var result = (0, _toConsumableArray2.default)(list);

  var _result$splice = result.splice(startIndex, 1),
      _result$splice2 = (0, _slicedToArray2.default)(_result$splice, 1),
      removed = _result$splice2[0];

  result.splice(endIndex, 0, removed);
  return result;
};

exports.euiDragDropReorder = euiDragDropReorder;

var euiDragDropMove = function euiDragDropMove(sourceList, destinationList, dropResultSource, dropResultDestination) {
  var _ref;

  var sourceClone = (0, _toConsumableArray2.default)(sourceList);
  var destClone = (0, _toConsumableArray2.default)(destinationList);

  var _sourceClone$splice = sourceClone.splice(dropResultSource.index, 1),
      _sourceClone$splice2 = (0, _slicedToArray2.default)(_sourceClone$splice, 1),
      removed = _sourceClone$splice2[0];

  destClone.splice(dropResultDestination.index, 0, removed);
  return _ref = {}, (0, _defineProperty2.default)(_ref, dropResultSource.droppableId, sourceClone), (0, _defineProperty2.default)(_ref, dropResultDestination.droppableId, destClone), _ref;
};

exports.euiDragDropMove = euiDragDropMove;

var euiDragDropCopy = function euiDragDropCopy(sourceList, destinationList, dropResultSource, dropResultDestination, idModification) {
  var _ref2;

  var sourceClone = (0, _toConsumableArray2.default)(sourceList);
  var destClone = (0, _toConsumableArray2.default)(destinationList);
  destClone.splice(dropResultDestination.index, 0, _objectSpread(_objectSpread({}, sourceList[dropResultSource.index]), {}, (0, _defineProperty2.default)({}, idModification.property, idModification.modifier())));
  return _ref2 = {}, (0, _defineProperty2.default)(_ref2, dropResultSource.droppableId, sourceClone), (0, _defineProperty2.default)(_ref2, dropResultDestination.droppableId, destClone), _ref2;
};

exports.euiDragDropCopy = euiDragDropCopy;