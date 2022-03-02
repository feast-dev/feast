function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useEffect } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { useDropzone } from 'react-dropzone';
import { EuiMarkdownEditorFooter } from './markdown_editor_footer';
import { useResizeObserver } from '../observer/resize_observer';
import { jsx as ___EmotionJSX } from "@emotion/react";

var getUnacceptedItems = function getUnacceptedItems(items, dropHandlers) {
  var unacceptedItems = [];

  for (var i = 0; i < items.length; i++) {
    var item = items[i];
    var isAccepted = false;

    for (var j = 0; j < dropHandlers.length; j++) {
      if (dropHandlers[j].accepts(item.type)) {
        isAccepted = true;
        break;
      }
    }

    if (!isAccepted) {
      unacceptedItems.push(item);
    }
  }

  return unacceptedItems;
};

export var EuiMarkdownEditorDropZone = function EuiMarkdownEditorDropZone(props) {
  var _React$useState = React.useState(false),
      _React$useState2 = _slicedToArray(_React$useState, 2),
      isDragging = _React$useState2[0],
      toggleDragging = _React$useState2[1];

  var _React$useState3 = React.useState(false),
      _React$useState4 = _slicedToArray(_React$useState3, 2),
      isUploadingFiles = _React$useState4[0],
      toggleUploadingFiles = _React$useState4[1];

  var _React$useState5 = React.useState(false),
      _React$useState6 = _slicedToArray(_React$useState5, 2),
      isDraggingError = _React$useState6[0],
      toggleDraggingError = _React$useState6[1];

  var children = props.children,
      uiPlugins = props.uiPlugins,
      errors = props.errors,
      dropHandlers = props.dropHandlers,
      insertText = props.insertText,
      hasUnacceptedItems = props.hasUnacceptedItems,
      setHasUnacceptedItems = props.setHasUnacceptedItems,
      setEditorFooterHeight = props.setEditorFooterHeight,
      isEditing = props.isEditing;
  var classes = classNames('euiMarkdownEditorDropZone', {
    'euiMarkdownEditorDropZone--isDragging': isDragging,
    'euiMarkdownEditorDropZone--hasError': hasUnacceptedItems,
    'euiMarkdownEditorDropZone--isDraggingError': isDraggingError
  });

  var _React$useState7 = React.useState(null),
      _React$useState8 = _slicedToArray(_React$useState7, 2),
      editorFooterRef = _React$useState8[0],
      setEditorFooterRef = _React$useState8[1];

  var _useResizeObserver = useResizeObserver(editorFooterRef, 'height'),
      editorFooterHeight = _useResizeObserver.height;

  useEffect(function () {
    if (editorFooterHeight !== 0) {
      setEditorFooterHeight(editorFooterHeight);
    }
  }, [setEditorFooterHeight, isEditing, editorFooterHeight]);

  var _useDropzone = useDropzone({
    disabled: dropHandlers.length === 0,
    // Disable click and keydown behavior
    noClick: true,
    noKeyboard: true,
    preventDropOnDocument: false,
    // multiple: false,
    onDragOver: function onDragOver(e) {
      var result;

      if (e.dataTransfer) {
        var unacceptedItems = getUnacceptedItems(e.dataTransfer.items, dropHandlers);
        setHasUnacceptedItems(unacceptedItems.length > 0);
        toggleDraggingError(unacceptedItems.length > 0);
        result = unacceptedItems.length === 0;
      } else {
        setHasUnacceptedItems(false);
        result = false;
      }

      toggleDragging(result);

      if (result === false) {
        e.preventDefault();
      }

      return result;
    },
    onDragEnter: function onDragEnter(e) {
      var result;

      if (e.dataTransfer) {
        var unacceptedItems = getUnacceptedItems(e.dataTransfer.items, dropHandlers);
        setHasUnacceptedItems(unacceptedItems.length > 0);
        toggleDraggingError(unacceptedItems.length > 0);
        result = unacceptedItems.length === 0;
      } else {
        setHasUnacceptedItems(false);
        result = false;
      }

      toggleDragging(result);

      if (result === false) {
        e.preventDefault();
      }

      return result;
    },
    onDragLeave: function onDragLeave() {
      toggleDragging(false);
    },
    onDrop: function onDrop(acceptedFiles) {
      var fileHandlers = []; // verify all files being dropped are supported

      preparation: for (var i = 0; i < acceptedFiles.length; i++) {
        var file = acceptedFiles[i];

        for (var j = 0; j < dropHandlers.length; j++) {
          if (dropHandlers[j].accepts(file.type)) {
            fileHandlers.push(dropHandlers[j]);
            continue preparation;
          }
        } // if we get here then a file isn't handled


        setHasUnacceptedItems(true);
        toggleDragging(false);
        toggleDraggingError(false);
        return;
      }

      toggleUploadingFiles(true);
      var resolved = [];

      for (var _i2 = 0; _i2 < acceptedFiles.length; _i2++) {
        var _file = acceptedFiles[_i2];
        var handler = fileHandlers[_i2];
        resolved.push(handler.getFormattingForItem(_file));
      }

      Promise.all(resolved).then(function (results) {
        results.forEach(function (_ref) {
          var text = _ref.text,
              config = _ref.config;
          return insertText(text, config);
        });
      }).catch(function () {}).then(function () {
        toggleDragging(false);
        toggleUploadingFiles(false);
        toggleDraggingError(false);
      });
    }
  }),
      getRootProps = _useDropzone.getRootProps,
      getInputProps = _useDropzone.getInputProps,
      open = _useDropzone.open;

  return ___EmotionJSX("div", _extends({}, getRootProps(), {
    className: classes
  }), children, ___EmotionJSX(EuiMarkdownEditorFooter, {
    ref: setEditorFooterRef,
    uiPlugins: uiPlugins,
    openFiles: function openFiles() {
      setHasUnacceptedItems(false);
      open();
    },
    isUploadingFiles: isUploadingFiles,
    hasUnacceptedItems: hasUnacceptedItems,
    dropHandlers: dropHandlers,
    errors: errors
  }), ___EmotionJSX("input", getInputProps()));
};
EuiMarkdownEditorDropZone.propTypes = {
  uiPlugins: PropTypes.arrayOf(PropTypes.shape({
    name: PropTypes.string.isRequired,
    button: PropTypes.shape({
      label: PropTypes.string.isRequired,
      iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]).isRequired
    }).isRequired,
    helpText: PropTypes.node,
    formatting: PropTypes.shape({
      prefix: PropTypes.string,
      suffix: PropTypes.string,
      blockPrefix: PropTypes.string,
      blockSuffix: PropTypes.string,
      multiline: PropTypes.bool,
      replaceNext: PropTypes.string,
      prefixSpace: PropTypes.bool,
      scanFor: PropTypes.string,
      surroundWithNewlines: PropTypes.bool,
      orderedList: PropTypes.bool,
      trimFirst: PropTypes.bool
    }),
    editor: PropTypes.elementType
  }).isRequired).isRequired,
  errors: PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.any.isRequired, PropTypes.any.isRequired]).isRequired).isRequired,
  dropHandlers: PropTypes.arrayOf(PropTypes.shape({
    supportedFiles: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    accepts: PropTypes.func.isRequired,
    getFormattingForItem: PropTypes.func.isRequired
  }).isRequired).isRequired,
  insertText: PropTypes.func.isRequired,
  hasUnacceptedItems: PropTypes.bool.isRequired,
  setHasUnacceptedItems: PropTypes.func.isRequired,
  setEditorFooterHeight: PropTypes.func.isRequired,
  isEditing: PropTypes.bool.isRequired
};