"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getColumnActionConfig = exports.isColumnActionEnabled = exports.getSortColumnActions = exports.getHideColumnAction = exports.getColumnActions = void 0;

var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));

var _react = _interopRequireDefault(require("react"));

var _i18n = require("../../../i18n");

var _data_grid_schema = require("../../utils/data_grid_schema");

var _column_sorting_draggable = require("../../controls/column_sorting_draggable");

var _react2 = require("@emotion/react");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

var getColumnActions = function getColumnActions(_ref) {
  var _column$actions;

  var column = _ref.column,
      columns = _ref.columns,
      schema = _ref.schema,
      schemaDetectors = _ref.schemaDetectors,
      setVisibleColumns = _ref.setVisibleColumns,
      focusFirstVisibleInteractiveCell = _ref.focusFirstVisibleInteractiveCell,
      setIsPopoverOpen = _ref.setIsPopoverOpen,
      sorting = _ref.sorting,
      switchColumnPos = _ref.switchColumnPos,
      setFocusedCell = _ref.setFocusedCell;

  if (column.actions === false) {
    return [];
  }

  var actions = [].concat((0, _toConsumableArray2.default)(getHideColumnAction({
    column: column,
    columns: columns,
    setVisibleColumns: setVisibleColumns,
    focusFirstVisibleInteractiveCell: focusFirstVisibleInteractiveCell
  })), (0, _toConsumableArray2.default)(getSortColumnActions({
    column: column,
    sorting: sorting,
    schema: schema,
    schemaDetectors: schemaDetectors
  })), (0, _toConsumableArray2.default)(getMoveColumnActions({
    column: column,
    columns: columns,
    switchColumnPos: switchColumnPos,
    setFocusedCell: setFocusedCell
  })), (0, _toConsumableArray2.default)(((_column$actions = column.actions) === null || _column$actions === void 0 ? void 0 : _column$actions.additional) || []));
  return actions.map(function (action) {
    return _objectSpread(_objectSpread({}, action), {}, {
      // Wrap EuiListGroupItem onClick function to close the popover and prevent bubbling up
      onClick: function onClick(e) {
        e.stopPropagation();
        setIsPopoverOpen(false);

        if (action === null || action === void 0 ? void 0 : action.onClick) {
          action.onClick(e);
        }
      }
    });
  });
};
/**
 * Hide column action
 */


exports.getColumnActions = getColumnActions;

var getHideColumnAction = function getHideColumnAction(_ref2) {
  var column = _ref2.column,
      columns = _ref2.columns,
      setVisibleColumns = _ref2.setVisibleColumns,
      focusFirstVisibleInteractiveCell = _ref2.focusFirstVisibleInteractiveCell;
  var items = [];

  var onClickHideColumn = function onClickHideColumn() {
    setVisibleColumns(columns.filter(function (col) {
      return col.id !== column.id;
    }).map(function (col) {
      return col.id;
    })); // Since we hid the current column, we need to manually set focus back onto the grid

    focusFirstVisibleInteractiveCell();
  };

  var action = {
    label: (0, _react2.jsx)(_i18n.EuiI18n, {
      token: "euiColumnActions.hideColumn",
      default: "Hide column"
    }),
    onClick: onClickHideColumn,
    iconType: 'eyeClosed',
    size: 'xs',
    color: 'text'
  };

  if (isColumnActionEnabled('showHide', column.actions)) {
    items.push(getColumnActionConfig(action, 'showHide', column.actions));
  }

  return items;
};
/**
 * Move column actions
 */


exports.getHideColumnAction = getHideColumnAction;

var getMoveColumnActions = function getMoveColumnActions(_ref3) {
  var column = _ref3.column,
      columns = _ref3.columns,
      switchColumnPos = _ref3.switchColumnPos,
      setFocusedCell = _ref3.setFocusedCell;
  var items = [];
  var colIdx = columns.findIndex(function (col) {
    return col.id === column.id;
  });

  if (isColumnActionEnabled('showMoveLeft', column.actions)) {
    var onClickMoveLeft = function onClickMoveLeft() {
      var targetCol = columns[colIdx - 1];

      if (targetCol) {
        switchColumnPos(column.id, targetCol.id);
        setFocusedCell([colIdx - 1, -1]);
      }
    };

    var action = {
      label: (0, _react2.jsx)(_i18n.EuiI18n, {
        token: "euiColumnActions.moveLeft",
        default: "Move left"
      }),
      iconType: 'sortLeft',
      size: 'xs',
      color: 'text',
      onClick: onClickMoveLeft,
      isDisabled: colIdx === 0
    };
    items.push(getColumnActionConfig(action, 'showMoveLeft', column.actions));
  }

  if (isColumnActionEnabled('showMoveRight', column.actions)) {
    var onClickMoveRight = function onClickMoveRight() {
      var targetCol = columns[colIdx + 1];

      if (targetCol) {
        switchColumnPos(column.id, targetCol.id);
        setFocusedCell([colIdx + 1, -1]);
      }
    };

    var _action = {
      label: (0, _react2.jsx)(_i18n.EuiI18n, {
        token: "euiColumnActions.moveRight",
        default: "Move right"
      }),
      iconType: 'sortRight',
      size: 'xs',
      color: 'text',
      onClick: onClickMoveRight,
      isDisabled: colIdx === columns.length - 1
    };
    items.push(getColumnActionConfig(_action, 'showMoveRight', column.actions));
  }

  return items;
};
/**
 * Sort column actions
 */


var getSortColumnActions = function getSortColumnActions(_ref4) {
  var column = _ref4.column,
      sorting = _ref4.sorting,
      schema = _ref4.schema,
      schemaDetectors = _ref4.schemaDetectors;
  if (!sorting) return [];
  var items = [];
  var sortingIdx = sorting.columns.findIndex(function (col) {
    return col.id === column.id;
  });
  var schemaDetails = schema.hasOwnProperty(column.id) && schema[column.id].columnType != null ? (0, _data_grid_schema.getDetailsForSchema)(schemaDetectors, schema[column.id].columnType) : null;

  var sortBy = function sortBy(direction) {
    var _sorting$columns$sort;

    if (sortingIdx >= 0 && ((_sorting$columns$sort = sorting.columns[sortingIdx]) === null || _sorting$columns$sort === void 0 ? void 0 : _sorting$columns$sort.direction) === direction) {
      // unsort if the same current and new direction are same
      var newColumns = sorting.columns.filter(function (_, idx) {
        return idx !== sortingIdx;
      });
      sorting.onSort(newColumns);
    } else if (sortingIdx >= 0) {
      // replace existing sort
      var _newColumns = Object.values(_objectSpread(_objectSpread({}, sorting.columns), {}, (0, _defineProperty2.default)({}, sortingIdx, {
        id: column.id,
        direction: direction
      })));

      sorting.onSort(_newColumns);
    } else {
      // add new sort
      var _newColumns2 = [].concat((0, _toConsumableArray2.default)(sorting.columns), [{
        id: column.id,
        direction: direction
      }]);

      sorting.onSort(_newColumns2);
    }
  };

  if (isColumnActionEnabled('showSortAsc', column.actions)) {
    var label = schemaDetails ? schemaDetails.sortTextAsc : _column_sorting_draggable.defaultSortAscLabel;

    var onClickSortAsc = function onClickSortAsc() {
      sortBy('asc');
    };

    var action = {
      label: (0, _react2.jsx)(_i18n.EuiI18n, {
        token: "euiColumnActions.sort",
        default: "Sort {schemaLabel}",
        values: {
          schemaLabel: label
        }
      }),
      onClick: onClickSortAsc,
      isDisabled: column.isSortable === false,
      className: sortingIdx >= 0 && sorting.columns[sortingIdx].direction === 'asc' ? 'euiDataGridHeader__action--selected' : '',
      iconType: 'sortUp',
      size: 'xs',
      color: 'text'
    };
    items.push(getColumnActionConfig(action, 'showSortAsc', column.actions));
  }

  if (isColumnActionEnabled('showSortDesc', column.actions)) {
    var _label = schemaDetails ? schemaDetails.sortTextDesc : _column_sorting_draggable.defaultSortDescLabel;

    var onClickSortDesc = function onClickSortDesc() {
      sortBy('desc');
    };

    var _action2 = {
      label: (0, _react2.jsx)(_i18n.EuiI18n, {
        token: "euiColumnActions.sort",
        default: "Sort {schemaLabel}",
        values: {
          schemaLabel: _label
        }
      }),
      onClick: onClickSortDesc,
      isDisabled: column.isSortable === false,
      className: sortingIdx >= 0 && sorting.columns[sortingIdx].direction === 'desc' ? 'euiDataGridHeader__action--selected' : '',
      iconType: 'sortDown',
      size: 'xs',
      color: 'text'
    };
    items.push(getColumnActionConfig(_action2, 'showSortDesc', column.actions));
  }

  return items;
};
/**
 * Column action utility helpers - mostly syntactical sugar for adding an extra
 * actions !== false checks, which we make an early return for in the main fn,
 * but that the individual utils don't know about and Typescript complains about
 */
// Check whether an action is enabled/should be appended to the actions array


exports.getSortColumnActions = getSortColumnActions;

var isColumnActionEnabled = function isColumnActionEnabled(actionKey, actions) {
  if (actions === false) return false;
  if ((actions === null || actions === void 0 ? void 0 : actions[actionKey]) === false) return false;
  return true;
}; // Utility helper for appending any custom EuiDataGridColumnActions configuration to its action


exports.isColumnActionEnabled = isColumnActionEnabled;

var getColumnActionConfig = function getColumnActionConfig(action, actionKey, actions) {
  var configuration = actions !== false && (actions === null || actions === void 0 ? void 0 : actions[actionKey]);
  return (0, _typeof2.default)(configuration) === 'object' ? _objectSpread(_objectSpread({}, action), configuration) : action;
};

exports.getColumnActionConfig = getColumnActionConfig;