function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import classNames from 'classnames';
import PropTypes from "prop-types";
import React from 'react';
import { EuiScreenReaderOnly } from '../../accessibility';
import { EuiButtonGroupButton } from './button_group_button';
import { colorToClassNameMap } from '../button';
import { useGeneratedHtmlId } from '../../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";
var groupSizeToClassNameMap = {
  s: '--small',
  m: '--medium',
  compressed: '--compressed'
};
export var EuiButtonGroup = function EuiButtonGroup(_ref) {
  var className = _ref.className,
      _ref$buttonSize = _ref.buttonSize,
      buttonSize = _ref$buttonSize === void 0 ? 's' : _ref$buttonSize,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'text' : _ref$color,
      _ref$idSelected = _ref.idSelected,
      idSelected = _ref$idSelected === void 0 ? '' : _ref$idSelected,
      _ref$idToSelectedMap = _ref.idToSelectedMap,
      idToSelectedMap = _ref$idToSelectedMap === void 0 ? {} : _ref$idToSelectedMap,
      _ref$isDisabled = _ref.isDisabled,
      isDisabled = _ref$isDisabled === void 0 ? false : _ref$isDisabled,
      _ref$isFullWidth = _ref.isFullWidth,
      isFullWidth = _ref$isFullWidth === void 0 ? false : _ref$isFullWidth,
      _ref$isIconOnly = _ref.isIconOnly,
      isIconOnly = _ref$isIconOnly === void 0 ? false : _ref$isIconOnly,
      legend = _ref.legend,
      name = _ref.name,
      onChange = _ref.onChange,
      _ref$options = _ref.options,
      options = _ref$options === void 0 ? [] : _ref$options,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'single' : _ref$type,
      rest = _objectWithoutProperties(_ref, ["className", "buttonSize", "color", "idSelected", "idToSelectedMap", "isDisabled", "isFullWidth", "isIconOnly", "legend", "name", "onChange", "options", "type"]);

  // Compressed style can't support `ghost` color because it's more like a form field than a button
  var badColorCombo = buttonSize === 'compressed' && color === 'ghost';
  var resolvedColor = badColorCombo ? 'text' : color;

  if (badColorCombo) {
    console.warn('EuiButtonGroup of compressed size does not support the ghost color. It will render as text instead.');
  }

  var classes = classNames('euiButtonGroup', "euiButtonGroup".concat(groupSizeToClassNameMap[buttonSize]), "euiButtonGroup".concat(colorToClassNameMap[resolvedColor]), {
    'euiButtonGroup--fullWidth': isFullWidth,
    'euiButtonGroup--isDisabled': isDisabled
  }, className);
  var typeIsSingle = type === 'single';
  var nameIfSingle = useGeneratedHtmlId({
    conditionalId: name
  });
  return ___EmotionJSX("fieldset", _extends({
    className: classes
  }, rest, {
    disabled: isDisabled
  }), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("legend", null, legend)), ___EmotionJSX("div", {
    className: "euiButtonGroup__buttons"
  }, options.map(function (option, index) {
    return ___EmotionJSX(EuiButtonGroupButton, _extends({
      key: index,
      name: nameIfSingle,
      isDisabled: isDisabled
    }, option, {
      element: typeIsSingle ? 'label' : 'button',
      isSelected: typeIsSingle ? option.id === idSelected : idToSelectedMap[option.id],
      color: resolvedColor,
      size: buttonSize,
      isIconOnly: isIconOnly,
      onChange: onChange
    }));
  })));
};
EuiButtonGroup.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Typical sizing is `s`. Medium `m` size should be reserved for major features.
     * `compressed` is meant to be used alongside and within compressed forms.
     */
  buttonSize: PropTypes.oneOf(["s", "m", "compressed"]),
  isDisabled: PropTypes.bool,

  /**
     * Expands the whole group to the full width of the container.
     * Each button gets equal widths no matter the content
     */
  isFullWidth: PropTypes.bool,

  /**
     * Hides the label to only show the `iconType` provided by the `option`
     */
  isIconOnly: PropTypes.bool,

  /**
     * A hidden group title (required for accessibility)
     */
  legend: PropTypes.string.isRequired,

  /**
     * Compressed styles don't support `ghost` color (Color will be changed to "text")
     */
  color: PropTypes.oneOf(["primary", "accent", "success", "warning", "danger", "ghost", "text"]),

  /**
           * Default for `type` is single so it can also be excluded
           */

  /**
     * Actual type is `'single' | 'multi'`.
     * Determines how the selection of the group should be handled.
     * With `'single'` only one option can be selected at a time (similar to radio group).
     * With `'multi'` multiple options selected (similar to checkbox group).
     */
  type: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.oneOf(["single", "multi"]), PropTypes.oneOf(["single"])]), PropTypes.oneOf(["multi"])]),

  /**
     * An array of #EuiButtonGroupOptionProps
     */
  options: PropTypes.arrayOf(PropTypes.shape({
    /**
       * Each option must have a unique `id` for maintaining selection
       */
    id: PropTypes.string.isRequired,

    /**
       * Each option must have a `label` even for icons which will be applied as the `aria-label`
       */
    label: PropTypes.node.isRequired,
    isDisabled: PropTypes.bool,

    /**
       * The value of the radio input.
       */
    value: PropTypes.any,

    /**
       * The type of the underlying HTML button
       */
    type: PropTypes.oneOf(["button", "submit", "reset"]),

    /**
       * Any `type` accepted by EuiIcon
       */
    iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

    /**
       * Can only be one side `left` or `right`
       */
    iconSide: PropTypes.oneOf(["left", "right"]),
    isLoading: PropTypes.bool,

    /**
       * Object of props passed to the <span/> wrapping the content's text/children only (not icon)
       */
    textProps: PropTypes.shape({
      className: PropTypes.string,
      "aria-label": PropTypes.string,
      "data-test-subj": PropTypes.string,
      ref: PropTypes.any,
      "data-text": PropTypes.string
    }),
    iconSize: PropTypes.oneOf(["s", "m"]),
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }).isRequired).isRequired,

  /**
           * The `name` attribute for radio inputs;
           * Defaults to a random string
           */
  name: PropTypes.string,

  /**
           * Styles the selected option to look selected (usually with `fill`)
           * Required by and only used in `type='single'`.
           */
  idSelected: PropTypes.string,

  /**
           * Multi: Returns the `id` of the clicked option
           */

  /**
           * Single: Returns the `id` of the clicked option and the `value`
           */
  onChange: PropTypes.func,

  /**
           * A map of `id`s as keys with the selected boolean values.
           * Required by and only used in `type='multi'`.
           */
  idToSelectedMap: PropTypes.shape({})
};