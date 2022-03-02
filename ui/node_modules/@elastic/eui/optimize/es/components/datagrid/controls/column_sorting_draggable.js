import _extends from "@babel/runtime/helpers/extends";
import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { EuiScreenReaderOnly } from '../../accessibility';
import { EuiButtonGroup, EuiButtonIcon } from '../../button';
import { EuiDraggable } from '../../drag_and_drop';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { EuiI18n } from '../../i18n';
import { EuiIcon } from '../../icon';
import { EuiText } from '../../text';
import { EuiToken } from '../../token';
import { getDetailsForSchema } from '../utils/data_grid_schema';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var defaultSortAscLabel = ___EmotionJSX(EuiI18n, {
  token: "euiColumnSortingDraggable.defaultSortAsc",
  default: "A-Z"
});
export var defaultSortDescLabel = ___EmotionJSX(EuiI18n, {
  token: "euiColumnSortingDraggable.defaultSortDesc",
  default: "Z-A"
});
export var EuiDataGridColumnSortingDraggable = function EuiDataGridColumnSortingDraggable(_ref) {
  var id = _ref.id,
      display = _ref.display,
      direction = _ref.direction,
      index = _ref.index,
      sorting = _ref.sorting,
      schema = _ref.schema,
      schemaDetectors = _ref.schemaDetectors,
      rest = _objectWithoutProperties(_ref, ["id", "display", "direction", "index", "sorting", "schema", "schemaDetectors"]);

  var schemaDetails = schema.hasOwnProperty(id) && schema[id].columnType != null ? getDetailsForSchema(schemaDetectors, schema[id].columnType) : null;
  var textSortAsc = schemaDetails != null ? schemaDetails.sortTextAsc : defaultSortAscLabel;
  var textSortDesc = schemaDetails != null ? schemaDetails.sortTextDesc : defaultSortDescLabel;
  var toggleOptions = [{
    id: "".concat(id, "Asc"),
    value: 'asc',
    label: textSortAsc,
    'data-test-subj': "euiDataGridColumnSorting-sortColumn-".concat(id, "-asc")
  }, {
    id: "".concat(id, "Desc"),
    value: 'desc',
    label: textSortDesc,
    'data-test-subj': "euiDataGridColumnSorting-sortColumn-".concat(id, "-desc")
  }];
  return ___EmotionJSX(EuiDraggable, _extends({
    draggableId: id,
    index: index
  }, rest), function (provided, state) {
    return ___EmotionJSX("div", {
      className: "euiDataGridColumnSorting__item ".concat(state.isDragging && 'euiDataGridColumnSorting__item-isDragging')
    }, ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", null, ___EmotionJSX(EuiI18n, {
      token: "euiColumnSortingDraggable.activeSortLabel",
      default: "{display} is sorting this data grid",
      values: {
        display: display
      }
    }, function (activeSortLabel) {
      return activeSortLabel;
    }))), ___EmotionJSX(EuiFlexGroup, {
      gutterSize: "xs",
      alignItems: "center",
      responsive: false,
      "data-test-subj": "euiDataGridColumnSorting-sortColumn-".concat(id)
    }, ___EmotionJSX(EuiFlexItem, {
      grow: false
    }, ___EmotionJSX(EuiI18n, {
      token: "euiColumnSortingDraggable.removeSortLabel",
      default: "Remove {display} from data grid sort",
      values: {
        display: display
      }
    }, function (removeSortLabel) {
      return ___EmotionJSX(EuiButtonIcon, {
        color: "text",
        className: "euiDataGridColumnSorting__button",
        "aria-label": removeSortLabel,
        iconType: "cross",
        onClick: function onClick() {
          var nextColumns = _toConsumableArray(sorting.columns);

          var columnIndex = nextColumns.map(function (_ref2) {
            var id = _ref2.id;
            return id;
          }).indexOf(id);
          nextColumns.splice(columnIndex, 1);
          sorting.onSort(nextColumns);
        }
      });
    })), ___EmotionJSX(EuiFlexItem, {
      grow: false
    }, ___EmotionJSX(EuiToken, {
      color: schemaDetails != null ? schemaDetails.color : undefined,
      iconType: schemaDetails != null ? schemaDetails.icon : 'tokenString'
    })), ___EmotionJSX(EuiFlexItem, {
      "aria-hidden": true
    }, ___EmotionJSX(EuiText, {
      size: "xs"
    }, ___EmotionJSX("p", null, display))), ___EmotionJSX(EuiFlexItem, {
      className: "euiDataGridColumnSorting__orderButtons"
    }, ___EmotionJSX(EuiI18n, {
      token: "euiColumnSortingDraggable.toggleLegend",
      default: "Select sorting method for {display}",
      values: {
        display: display
      }
    }, function (toggleLegend) {
      return ___EmotionJSX(EuiButtonGroup, {
        legend: toggleLegend,
        name: id,
        isFullWidth: true,
        options: toggleOptions,
        buttonSize: "compressed",
        className: "euiDataGridColumnSorting__order",
        idSelected: direction === 'asc' ? "".concat(id, "Asc") : "".concat(id, "Desc"),
        onChange: function onChange(_, direction) {
          var nextColumns = _toConsumableArray(sorting.columns);

          var columnIndex = nextColumns.map(function (_ref3) {
            var id = _ref3.id;
            return id;
          }).indexOf(id);
          nextColumns.splice(columnIndex, 1, {
            id: id,
            direction: direction
          });
          sorting.onSort(nextColumns);
        }
      });
    })), ___EmotionJSX(EuiFlexItem, _extends({
      grow: false
    }, provided.dragHandleProps), ___EmotionJSX("div", provided.dragHandleProps, ___EmotionJSX(EuiIcon, {
      type: "grab",
      color: "subdued"
    })))));
  });
};