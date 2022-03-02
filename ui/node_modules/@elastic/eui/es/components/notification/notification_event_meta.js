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
import React, { useState } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { EuiIcon } from '../icon';
import { EuiBadge } from '../badge';
import { EuiPopover } from '../popover';
import { EuiButtonIcon } from '../button';
import { EuiContextMenuPanel } from '../context_menu';
import { EuiI18n } from '../i18n';
import { useGeneratedHtmlId } from '../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiNotificationEventMeta = function EuiNotificationEventMeta(_ref) {
  var id = _ref.id,
      iconType = _ref.iconType,
      type = _ref.type,
      time = _ref.time,
      _ref$badgeColor = _ref.badgeColor,
      badgeColor = _ref$badgeColor === void 0 ? 'hollow' : _ref$badgeColor,
      severity = _ref.severity,
      eventName = _ref.eventName,
      iconAriaLabel = _ref.iconAriaLabel,
      onOpenContextMenu = _ref.onOpenContextMenu;

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      isPopoverOpen = _useState2[0],
      setIsPopoverOpen = _useState2[1];

  var classes = classNames('euiNotificationEventMeta', {
    'euiNotificationEventMeta--hasContextMenu': onOpenContextMenu
  });

  var _useState3 = useState([]),
      _useState4 = _slicedToArray(_useState3, 2),
      contextMenuItems = _useState4[0],
      setContextMenuItems = _useState4[1];

  var randomPopoverId = useGeneratedHtmlId();
  var ariaAttribute = iconAriaLabel ? {
    'aria-label': iconAriaLabel
  } : {
    'aria-hidden': true
  };

  var onOpenPopover = function onOpenPopover() {
    setIsPopoverOpen(!isPopoverOpen);

    if (onOpenContextMenu) {
      setContextMenuItems(onOpenContextMenu());
    }
  };

  return ___EmotionJSX("div", {
    className: classes
  }, ___EmotionJSX("div", {
    className: "euiNotificationEventMeta__section"
  }, iconType && ___EmotionJSX(EuiIcon, _extends({
    className: "euiNotificationEventMeta__icon",
    type: iconType
  }, ariaAttribute)), type && ___EmotionJSX(EuiBadge, {
    className: "euiNotificationEventMeta__badge",
    color: badgeColor
  }, severity ? "".concat(type, ": ").concat(severity) : type)), ___EmotionJSX("div", {
    className: "euiNotificationEventMeta__section"
  }, ___EmotionJSX("span", {
    className: "euiNotificationEventMeta__time"
  }, time)), onOpenContextMenu && ___EmotionJSX("div", {
    className: "euiNotificationEventMeta__contextMenuWrapper"
  }, ___EmotionJSX(EuiPopover, {
    id: randomPopoverId,
    ownFocus: true,
    repositionOnScroll: true,
    isOpen: isPopoverOpen,
    panelPaddingSize: "none",
    anchorPosition: "leftUp",
    button: ___EmotionJSX(EuiI18n, {
      token: "euiNotificationEventMeta.contextMenuButton",
      default: "Menu for {eventName}",
      values: {
        eventName: eventName
      }
    }, function (contextMenuButton) {
      return ___EmotionJSX(EuiButtonIcon, {
        "aria-label": contextMenuButton,
        "aria-controls": randomPopoverId,
        "aria-expanded": isPopoverOpen,
        "aria-haspopup": "true",
        iconType: "boxesVertical",
        color: "text",
        onClick: onOpenPopover,
        "data-test-subj": "".concat(id, "-notificationEventMetaButton")
      });
    }),
    closePopover: function closePopover() {
      return setIsPopoverOpen(false);
    }
  }, ___EmotionJSX("div", {
    onClick: function onClick() {
      return setIsPopoverOpen(false);
    }
  }, ___EmotionJSX(EuiContextMenuPanel, {
    items: contextMenuItems
  })))));
};
EuiNotificationEventMeta.propTypes = {
  id: PropTypes.string.isRequired,

  /**
     * Type of event (e.g. "Alert", "Cloud", etc..). Shows inside a badge.
     */
  type: PropTypes.string.isRequired,

  /**
     * A unique, human-friendly name for the event to be used in aria attributes (e.g. "alert-critical-01", "cloud-no-severity-12", etc..).
     */
  eventName: PropTypes.string.isRequired,

  /**
     * Type of severity (e.g. "Critical", "Warning", etc..). Shows as a text after the `type` following the format "Alert: Critical".
     */
  severity: PropTypes.string,

  /**
     * Accepts either our palette colors (primary, success ..etc) or a hex value `#FFFFFF`, `#000`.
     */
  badgeColor: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.oneOf(["default", "primary", "success", "accent", "warning", "danger", "text", "subdued", "ghost", "inherit"]).isRequired]),

  /**
     * The icon used to visually represent this data type. Accepts any `EuiIcon IconType`.
     */
  iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

  /**
     * Specify an `aria-label` for the icon.
     * If no `aria-label` is passed we assume the icon is purely decorative.
     */
  iconAriaLabel: PropTypes.string,

  /**
     * Indicates when the event was received.
     */
  time: PropTypes.node.isRequired,

  /**
     * Necessary to trigger `onOpenContextMenu` from #EuiNotificationEvent
     */
  onOpenContextMenu: PropTypes.func
};