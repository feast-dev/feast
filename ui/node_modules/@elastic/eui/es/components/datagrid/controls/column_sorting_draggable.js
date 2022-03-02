function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _toConsumableArray(arr) { return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _unsupportedIterableToArray(arr) || _nonIterableSpread(); }

function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _iterableToArray(iter) { if (typeof Symbol !== "undefined" && Symbol.iterator in Object(iter)) return Array.from(iter); }

function _arrayWithoutHoles(arr) { if (Array.isArray(arr)) return _arrayLikeToArray(arr); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import PropTypes from "prop-types";
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
EuiDataGridColumnSortingDraggable.propTypes = {
  id: PropTypes.string.isRequired,
  direction: PropTypes.string.isRequired,
  index: PropTypes.number.isRequired,
  sorting: PropTypes.shape({
    /**
       * A function that receives updated column sort details in response to user interactions in the toolbar controls
       */
    onSort: PropTypes.func.isRequired,

    /**
       * An array of the column ids currently being sorted and their sort direction. The array order determines the sort order. `{ id: 'A'; direction: 'asc' }`
       */
    columns: PropTypes.arrayOf(PropTypes.shape({
      id: PropTypes.string.isRequired,
      direction: PropTypes.oneOf(["asc", "desc"]).isRequired
    }).isRequired).isRequired
  }).isRequired,
  schema: PropTypes.shape({}).isRequired,
  schemaDetectors: PropTypes.arrayOf(PropTypes.shape({
    /**
       * The name of this data type, matches #EuiDataGridColumn / schema `schema`
       */
    type: PropTypes.string.isRequired,

    /**
       * The function given the text value of a cell and returns a score of [0...1] of how well the value matches this data type
       */
    detector: PropTypes.func.isRequired,

    /**
       * A custom comparator function when performing in-memory sorting on this data type, takes `(a: string, b: string, direction: 'asc' | 'desc) => -1 | 0 | 1`
       */
    comparator: PropTypes.func,

    /**
       * The icon used to visually represent this data type. Accepts any `EuiIcon IconType`.
       */
    icon: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]).isRequired,

    /**
       * The color associated with this data type; it's used to color the icon token
       */
    color: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.oneOf(["euiColorVis0", "euiColorVis1", "euiColorVis2", "euiColorVis3", "euiColorVis4", "euiColorVis5", "euiColorVis6", "euiColorVis7", "euiColorVis8", "euiColorVis9", "gray"]).isRequired, PropTypes.string.isRequired]).isRequired, PropTypes.string.isRequired]),

    /**
       * Text for how to represent an ascending sort of this data type, e.g. 'A -> Z'
       */
    sortTextAsc: PropTypes.node.isRequired,

    /**
       * Text for how to represent a descending sort of this data type, e.g. 'Z -> A'
       */
    sortTextDesc: PropTypes.node.isRequired,

    /**
       * Whether columns with this schema are sortable (defaults to true). Can be overridden at the individual #EuiDataGridColumn level
       */
    isSortable: PropTypes.bool,

    /**
       * This property controls the capitalization of text
       */
    textTransform: PropTypes.oneOf(["uppercase", "lowercase", "capitalize"]),

    /**
       * Default sort direction of columns with this schema. Can be overridden at the individual #EuiDataGridColumn level
       */
    defaultSortDirection: PropTypes.oneOf(["asc", "desc"])
  }).isRequired).isRequired,

  /**
     * Value to be shown in column sorting popover.
     */
  display: PropTypes.string.isRequired
};