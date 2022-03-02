function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useContext, forwardRef } from 'react';
import PropTypes from "prop-types";
import { EuiButtonEmpty, EuiButtonIcon } from '../button';
import { EuiI18n } from '../i18n';
import { EuiToolTip } from '../tool_tip';
import { MODE_VIEWING } from './markdown_modes';
import { EuiMarkdownContext } from './markdown_context';
// @ts-ignore a react svg
import MarkdownCheckmarkIcon from './icons/markdown_checkmark';
import { jsx as ___EmotionJSX } from "@emotion/react";
var boldItalicButtons = [{
  id: 'mdBold',
  label: 'Bold',
  name: 'bold',
  iconType: 'editorBold'
}, {
  id: 'mdItalic',
  label: 'Italic',
  name: 'italic',
  iconType: 'editorItalic'
}];
var listButtons = [{
  id: 'mdUl',
  label: 'Unordered list',
  name: 'ul',
  iconType: 'editorUnorderedList'
}, {
  id: 'mdOl',
  label: 'Ordered list',
  name: 'ol',
  iconType: 'editorOrderedList'
}, {
  id: 'mdTl',
  label: 'Task list',
  name: 'tl',
  iconType: MarkdownCheckmarkIcon
}];
var quoteCodeLinkButtons = [{
  id: 'mdQuote',
  label: 'Quote',
  name: 'quote',
  iconType: 'quote'
}, {
  id: 'mdCode',
  label: 'Code',
  name: 'code',
  iconType: 'editorCodeBlock'
}, {
  id: 'mdLink',
  label: 'Link',
  name: 'link',
  iconType: 'editorLink'
}];
export var EuiMarkdownEditorToolbar = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var markdownActions = _ref.markdownActions,
      viewMode = _ref.viewMode,
      onClickPreview = _ref.onClickPreview,
      uiPlugins = _ref.uiPlugins,
      selectedNode = _ref.selectedNode;

  var _useContext = useContext(EuiMarkdownContext),
      openPluginEditor = _useContext.openPluginEditor;

  var handleMdButtonClick = function handleMdButtonClick(mdButtonId) {
    var actionResult = markdownActions.do(mdButtonId);
    if (actionResult !== true) openPluginEditor(actionResult);
  };

  var isPreviewing = viewMode === MODE_VIEWING;
  return ___EmotionJSX("div", {
    ref: ref,
    className: "euiMarkdownEditorToolbar"
  }, ___EmotionJSX("div", {
    className: "euiMarkdownEditorToolbar__buttons"
  }, boldItalicButtons.map(function (item) {
    return ___EmotionJSX(EuiToolTip, {
      key: item.id,
      content: item.label,
      delay: "long"
    }, ___EmotionJSX(EuiButtonIcon, {
      color: "text",
      onClick: function onClick() {
        return handleMdButtonClick(item.id);
      },
      iconType: item.iconType,
      "aria-label": item.label,
      isDisabled: isPreviewing
    }));
  }), ___EmotionJSX("span", {
    className: "euiMarkdownEditorToolbar__divider"
  }), listButtons.map(function (item) {
    return ___EmotionJSX(EuiToolTip, {
      key: item.id,
      content: item.label,
      delay: "long"
    }, ___EmotionJSX(EuiButtonIcon, {
      color: "text",
      onClick: function onClick() {
        return handleMdButtonClick(item.id);
      },
      iconType: item.iconType,
      "aria-label": item.label,
      isDisabled: isPreviewing
    }));
  }), ___EmotionJSX("span", {
    className: "euiMarkdownEditorToolbar__divider"
  }), quoteCodeLinkButtons.map(function (item) {
    return ___EmotionJSX(EuiToolTip, {
      key: item.id,
      content: item.label,
      delay: "long"
    }, ___EmotionJSX(EuiButtonIcon, {
      color: "text",
      onClick: function onClick() {
        return handleMdButtonClick(item.id);
      },
      iconType: item.iconType,
      "aria-label": item.label,
      isDisabled: isPreviewing
    }));
  }), uiPlugins.length > 0 ? ___EmotionJSX(React.Fragment, null, ___EmotionJSX("span", {
    className: "euiMarkdownEditorToolbar__divider"
  }), uiPlugins.map(function (_ref2) {
    var name = _ref2.name,
        button = _ref2.button;
    var isSelectedNodeType = selectedNode && selectedNode.type === name;
    return ___EmotionJSX(EuiToolTip, {
      key: name,
      content: button.label,
      delay: "long"
    }, ___EmotionJSX(EuiButtonIcon, _extends({
      color: "text"
    }, isSelectedNodeType ? {
      style: {
        background: 'rgba(0, 0, 0, 0.15)'
      }
    } : null, {
      onClick: function onClick() {
        return handleMdButtonClick(name);
      },
      iconType: button.iconType,
      "aria-label": button.label,
      isDisabled: isPreviewing
    })));
  })) : null), isPreviewing ? ___EmotionJSX(EuiButtonEmpty, {
    iconType: "editorCodeBlock",
    color: "text",
    size: "s",
    onClick: onClickPreview
  }, ___EmotionJSX(EuiI18n, {
    token: "euiMarkdownEditorToolbar.editor",
    default: "Editor"
  })) : ___EmotionJSX(EuiButtonEmpty, {
    iconType: "eye",
    color: "text",
    size: "s",
    onClick: onClickPreview
  }, ___EmotionJSX(EuiI18n, {
    token: "euiMarkdownEditorToolbar.previewMarkdown",
    default: "Preview"
  })));
});
EuiMarkdownEditorToolbar.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  selectedNode: PropTypes.oneOfType([PropTypes.oneOf([null]), PropTypes.any.isRequired]),
  markdownActions: PropTypes.any.isRequired,
  viewMode: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.any.isRequired]).isRequired,
  onClickPreview: PropTypes.func.isRequired,
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
  }).isRequired).isRequired
};
EuiMarkdownEditorToolbar.displayName = 'EuiMarkdownEditorToolbar';