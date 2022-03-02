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
import React from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { EuiBetaBadge } from '../badge/beta_badge';
import { getSecureRelForTarget, useGeneratedHtmlId } from '../../services';
import { EuiRadio, EuiCheckbox } from '../form';
import { validateHref } from '../../services/security/href_validator';
import { EuiToolTip } from '../tool_tip';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiKeyPadMenuItem = function EuiKeyPadMenuItem(_ref) {
  var id = _ref.id,
      isSelected = _ref.isSelected,
      _isDisabled = _ref.isDisabled,
      label = _ref.label,
      children = _ref.children,
      className = _ref.className,
      betaBadgeLabel = _ref.betaBadgeLabel,
      betaBadgeTooltipContent = _ref.betaBadgeTooltipContent,
      betaBadgeIconType = _ref.betaBadgeIconType,
      betaBadgeTooltipProps = _ref.betaBadgeTooltipProps,
      href = _ref.href,
      rel = _ref.rel,
      target = _ref.target,
      buttonRef = _ref.buttonRef,
      checkable = _ref.checkable,
      name = _ref.name,
      value = _ref.value,
      disabled = _ref.disabled,
      _onChange = _ref.onChange,
      rest = _objectWithoutProperties(_ref, ["id", "isSelected", "isDisabled", "label", "children", "className", "betaBadgeLabel", "betaBadgeTooltipContent", "betaBadgeIconType", "betaBadgeTooltipProps", "href", "rel", "target", "buttonRef", "checkable", "name", "value", "disabled", "onChange"]);

  var isHrefValid = !href || validateHref(href);
  var isDisabled = disabled || _isDisabled || !isHrefValid;
  var classes = classNames('euiKeyPadMenuItem', {
    'euiKeyPadMenuItem--hasBetaBadge': betaBadgeLabel,
    'euiKeyPadMenuItem--checkable': checkable,
    'euiKeyPadMenuItem-isDisabled': isDisabled,
    'euiKeyPadMenuItem-isSelected': isSelected
  }, className);
  var Element = href && !isDisabled ? 'a' : 'button';
  if (checkable) Element = 'label';
  var itemId = useGeneratedHtmlId({
    conditionalId: id
  });

  var renderCheckableElement = function renderCheckableElement() {
    if (!checkable) return;
    var inputClasses = classNames('euiKeyPadMenuItem__checkableInput');
    var checkableElement;

    if (checkable === 'single') {
      checkableElement = ___EmotionJSX(EuiRadio, {
        id: itemId,
        className: inputClasses,
        checked: isSelected,
        disabled: isDisabled,
        name: name,
        value: value,
        onChange: function onChange() {
          return _onChange(itemId, value);
        }
      });
    } else {
      checkableElement = ___EmotionJSX(EuiCheckbox, {
        id: itemId,
        className: inputClasses,
        checked: isSelected,
        disabled: isDisabled,
        name: name,
        onChange: function onChange() {
          return _onChange(itemId);
        }
      });
    }

    return checkableElement;
  };

  var renderBetaBadge = function renderBetaBadge() {
    if (!betaBadgeLabel) return;
    return ___EmotionJSX(EuiBetaBadge // Since we move the tooltip contents to a wrapping EuiToolTip,
    // this badge is purely visual therefore we can safely hide it from screen readers
    , {
      "aria-hidden": "true",
      size: "s",
      color: "subdued",
      className: "euiKeyPadMenuItem__betaBadge",
      label: betaBadgeLabel.charAt(0),
      iconType: betaBadgeIconType
    });
  };

  var relObj = {};

  if (href && !isDisabled) {
    relObj.href = href;
    relObj.rel = getSecureRelForTarget({
      href: href,
      target: target,
      rel: rel
    });
    relObj.target = target;
    relObj['aria-current'] = isSelected ? isSelected : undefined;
  } else if (checkable) {
    relObj.htmlFor = itemId;
  } else {
    relObj.disabled = isDisabled;
    relObj.type = 'button';
    relObj['aria-pressed'] = isSelected;
  }

  var button = ___EmotionJSX(Element, _extends({
    className: classes
  }, relObj, rest, {
    // Unable to get past `LegacyRef` conflicts
    ref: buttonRef
  }), ___EmotionJSX("span", {
    className: "euiKeyPadMenuItem__inner"
  }, checkable ? renderCheckableElement() : renderBetaBadge(), ___EmotionJSX("span", {
    className: "euiKeyPadMenuItem__icon"
  }, children), ___EmotionJSX("span", {
    className: "euiKeyPadMenuItem__label"
  }, label)));

  return betaBadgeLabel ? ___EmotionJSX(EuiToolTip, _extends({}, betaBadgeTooltipProps, {
    title: betaBadgeLabel,
    content: betaBadgeTooltipContent,
    delay: "long"
  }), button) : button;
};
EuiKeyPadMenuItem.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * One will be generated if not provided
     */

  /**
     * One will be generated if not provided
     */

  /**
     * One will be generated if not provided
     */
  id: PropTypes.string,

  /**
     * Pass an EuiIcon, preferrably `size="l"`
     */

  /**
     * Pass an EuiIcon, preferrably `size="l"`
     */

  /**
     * Pass an EuiIcon, preferrably `size="l"`
     */
  children: PropTypes.node.isRequired,
  isDisabled: PropTypes.bool,

  /**
     * Indicate if an item is the current one.
     * Be sure to use `true` AND `false` when acting as a toggle to ensure the attribute is added for both states
     */

  /**
     * Indicate if an item is the current one.
     * Be sure to use `true` AND `false` when acting as a toggle to ensure the attribute is added for both states
     */

  /**
     * Indicate if an item is the current one.
     * Be sure to use `true` AND `false` when acting as a toggle to ensure the attribute is added for both states
     */
  isSelected: PropTypes.bool,

  /**
     * The text to display beneath the icon
     */

  /**
     * The text to display beneath the icon
     */

  /**
     * The text to display beneath the icon
     */
  label: PropTypes.node.isRequired,

  /**
     * Beta badges are unavailable if the item is checkable
     */

  /**
     * Beta badges are unavailable if the item is checkable
     */

  /**
         * Type `'multi'` renders the item as a `<label>` and
         * adds a checkbox.
         */

  /**
         * Type `'single'` renders the item as a `<label>` and
         * adds a radio element.
         */
  checkable: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.oneOf(["single"]).isRequired, PropTypes.oneOf(["multi"]).isRequired]).isRequired, PropTypes.oneOf([undefined])]),

  /**
         * The `name` attribute for radio inputs;
         * Required in order to group properly
         */
  name: PropTypes.string,

  /**
         * The value of the radio input for 'single'
         */
  value: PropTypes.string,

  /**
         * Multi: Returns the `id` of the clicked option
         */

  /**
         * Single: Returns the `id` of the clicked option and the `value`
         */
  onChange: PropTypes.func,
  href: PropTypes.string,
  onClick: PropTypes.func,
  buttonRef: PropTypes.any,
  rel: PropTypes.string,

  /**
     * Add a badge to the card to label it as "Beta" or other non-GA state
     */

  /**
     * Add a badge to the card to label it as "Beta" or other non-GA state
     */
  betaBadgeLabel: PropTypes.string,

  /**
     * Supply an icon type if the badge should just be an icon
     */
  betaBadgeIconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

  /**
     * Add a description to the beta badge (will appear in a tooltip)
     */

  /**
     * Add a description to the beta badge (will appear in a tooltip)
     */
  betaBadgeTooltipContent: PropTypes.node,

  /**
     * Extends the wrapping EuiToolTip props when `betaBadgeLabel` is provided
     */

  /**
     * Extends the wrapping EuiToolTip props when `betaBadgeLabel` is provided
     */
  betaBadgeTooltipProps: PropTypes.any
};