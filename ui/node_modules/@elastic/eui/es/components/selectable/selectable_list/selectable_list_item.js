function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function"); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, writable: true, configurable: true } }); if (superClass) _setPrototypeOf(subClass, superClass); }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _possibleConstructorReturn(self, call) { if (call && (_typeof(call) === "object" || typeof call === "function")) { return call; } return _assertThisInitialized(self); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import classNames from 'classnames';
import PropTypes from "prop-types";
import React, { Component } from 'react';
import { EuiI18n } from '../../i18n';
import { EuiIcon } from '../../icon';
import { EuiScreenReaderOnly } from '../../accessibility';
import { EuiBadge } from '../../badge';
import { jsx as ___EmotionJSX } from "@emotion/react";

function resolveIconAndColor(checked) {
  if (!checked) {
    return {
      icon: 'empty'
    };
  }

  return checked === 'on' ? {
    icon: 'check',
    color: 'text'
  } : {
    icon: 'cross',
    color: 'text'
  };
}

// eslint-disable-next-line react/prefer-stateless-function
export var EuiSelectableListItem = /*#__PURE__*/function (_Component) {
  _inherits(EuiSelectableListItem, _Component);

  var _super = _createSuper(EuiSelectableListItem);

  function EuiSelectableListItem(props) {
    _classCallCheck(this, EuiSelectableListItem);

    return _super.call(this, props);
  }

  _createClass(EuiSelectableListItem, [{
    key: "render",
    value: function render() {
      var _this$props = this.props,
          children = _this$props.children,
          className = _this$props.className,
          disabled = _this$props.disabled,
          checked = _this$props.checked,
          isFocused = _this$props.isFocused,
          showIcons = _this$props.showIcons,
          prepend = _this$props.prepend,
          append = _this$props.append,
          allowExclusions = _this$props.allowExclusions,
          onFocusBadge = _this$props.onFocusBadge,
          rest = _objectWithoutProperties(_this$props, ["children", "className", "disabled", "checked", "isFocused", "showIcons", "prepend", "append", "allowExclusions", "onFocusBadge"]);

      var classes = classNames('euiSelectableListItem', {
        'euiSelectableListItem-isFocused': isFocused
      }, className);
      var optionIcon;

      if (showIcons) {
        var _resolveIconAndColor = resolveIconAndColor(checked),
            icon = _resolveIconAndColor.icon,
            color = _resolveIconAndColor.color;

        optionIcon = ___EmotionJSX(EuiIcon, {
          className: "euiSelectableListItem__icon",
          color: color,
          type: icon
        });
      }

      var state;
      var instruction;

      if (allowExclusions && checked === 'on') {
        state = ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, ___EmotionJSX(EuiI18n, {
          token: "euiSelectableListItem.includedOption",
          default: "Included option."
        })));
        instruction = ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, ___EmotionJSX(EuiI18n, {
          token: "euiSelectableListItem.includedOptionInstructions",
          default: "To exclude this option, press enter."
        })));
      } else if (allowExclusions && checked === 'off') {
        state = ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, ___EmotionJSX(EuiI18n, {
          token: "euiSelectableListItem.excludedOption",
          default: "Excluded option."
        })));
        instruction = ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, ___EmotionJSX(EuiI18n, {
          token: "euiSelectableListItem.excludedOptionInstructions",
          default: "To deselect this option, press enter."
        })));
      }

      var prependNode;

      if (prepend) {
        prependNode = ___EmotionJSX("span", {
          className: "euiSelectableListItem__prepend"
        }, prepend);
      }

      var appendNode;

      if (append || !!onFocusBadge) {
        var onFocusBadgeNode;
        var defaultOnFocusBadgeProps = {
          'aria-hidden': true,
          iconType: 'returnKey',
          iconSide: 'left',
          color: 'hollow'
        };

        if (onFocusBadge === true) {
          onFocusBadgeNode = ___EmotionJSX(EuiBadge, _extends({
            className: "euiSelectableListItem__onFocusBadge"
          }, defaultOnFocusBadgeProps));
        } else if (!!onFocusBadge && onFocusBadge !== false) {
          var _children = onFocusBadge.children,
              _className = onFocusBadge.className,
              restBadgeProps = _objectWithoutProperties(onFocusBadge, ["children", "className"]);

          onFocusBadgeNode = ___EmotionJSX(EuiBadge, _extends({
            className: classNames('euiSelectableListItem__onFocusBadge', _className)
          }, defaultOnFocusBadgeProps, restBadgeProps), _children);
        } // Only display the append wrapper if append exists or isFocused


        if (append || isFocused && !disabled) {
          appendNode = ___EmotionJSX("span", {
            className: "euiSelectableListItem__append"
          }, append, " ", isFocused && !disabled ? onFocusBadgeNode : null);
        }
      }

      return ___EmotionJSX("li", _extends({
        // eslint-disable-next-line jsx-a11y/no-noninteractive-element-to-interactive-role
        role: "option",
        "aria-selected": !disabled && typeof checked === 'string',
        className: classes,
        "aria-disabled": disabled
      }, rest), ___EmotionJSX("span", {
        className: "euiSelectableListItem__content"
      }, optionIcon, prependNode, ___EmotionJSX("span", {
        className: "euiSelectableListItem__text"
      }, state, children, instruction), appendNode));
    }
  }]);

  return EuiSelectableListItem;
}(Component);

_defineProperty(EuiSelectableListItem, "defaultProps", {
  showIcons: true,
  onFocusBadge: true
});

EuiSelectableListItem.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  children: PropTypes.node,

  /**
       * Applies an icon and visual styling to activated items
       */
  checked: PropTypes.oneOf(["on", "off", undefined]),

  /**
       * Shows icons based on `checked` type
       */
  showIcons: PropTypes.bool,

  /**
       * Highlights the item for pseudo focus
       */
  isFocused: PropTypes.bool,
  disabled: PropTypes.bool,
  prepend: PropTypes.node,
  append: PropTypes.node,
  allowExclusions: PropTypes.bool,

  /**
       * When enabled by setting to either `true` or passing custom a custom badge,
       * shows a hollow badge as an append (far right) when the item is focused.
       * The default content when `true` is `↩ to select/deselect/include/exclude`
       */
  onFocusBadge: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.shape({
    /**
       * Accepts any string from our icon library
       */
    iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

    /**
       * The side of the badge the icon should sit
       */
    iconSide: PropTypes.oneOf(["left", "right"]),

    /**
       * Accepts either our palette colors (primary, success ..etc) or a hex value `#FFFFFF`, `#000`.
       */
    color: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.oneOf(["default", "primary", "success", "accent", "warning", "danger", "text", "subdued", "ghost", "inherit"]).isRequired]),

    /**
       * Will override any color passed through the `color` prop.
       */
    isDisabled: PropTypes.bool,

    /**
       * Props passed to the close button.
       */
    closeButtonProps: PropTypes.any,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,

    /**
       * Will apply an onclick to icon within the badge
       */
    iconOnClick: PropTypes.func,

    /**
       * Aria label applied to the iconOnClick button
       */
    iconOnClickAriaLabel: PropTypes.any,

    /**
       * Will apply an onclick to the badge itself
       */
    onClick: PropTypes.func,

    /**
       * Aria label applied to the onClick button
       */
    onClickAriaLabel: PropTypes.any,
    href: PropTypes.string,
    target: PropTypes.string,
    rel: PropTypes.string
  }).isRequired])
};