function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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
import React, { Component } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { keysOf } from '../common';
import { Timer } from '../../services/time';
import { EuiGlobalToastListItem } from './global_toast_list_item';
import { EuiToast } from './toast';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sideToClassNameMap = {
  left: 'euiGlobalToastList--left',
  right: 'euiGlobalToastList--right'
};
export var SIDES = keysOf(sideToClassNameMap);
export var TOAST_FADE_OUT_MS = 250;
export var EuiGlobalToastList = /*#__PURE__*/function (_Component) {
  _inherits(EuiGlobalToastList, _Component);

  var _super = _createSuper(EuiGlobalToastList);

  function EuiGlobalToastList() {
    var _this;

    _classCallCheck(this, EuiGlobalToastList);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "state", {
      toastIdToDismissedMap: {}
    });

    _defineProperty(_assertThisInitialized(_this), "dismissTimeoutIds", []);

    _defineProperty(_assertThisInitialized(_this), "toastIdToTimerMap", {});

    _defineProperty(_assertThisInitialized(_this), "isScrollingToBottom", false);

    _defineProperty(_assertThisInitialized(_this), "isScrolledToBottom", true);

    _defineProperty(_assertThisInitialized(_this), "isUserInteracting", false);

    _defineProperty(_assertThisInitialized(_this), "isScrollingAnimationFrame", 0);

    _defineProperty(_assertThisInitialized(_this), "startScrollingAnimationFrame", 0);

    _defineProperty(_assertThisInitialized(_this), "listElement", null);

    _defineProperty(_assertThisInitialized(_this), "onMouseEnter", function () {
      // Stop scrolling to bottom if we're in mid-scroll, because the user wants to interact with
      // the list.
      _this.isScrollingToBottom = false;
      _this.isUserInteracting = true; // Don't let toasts dismiss themselves while the user is interacting with them.

      for (var _toastId in _this.toastIdToTimerMap) {
        if (_this.toastIdToTimerMap.hasOwnProperty(_toastId)) {
          var timer = _this.toastIdToTimerMap[_toastId];
          timer.pause();
        }
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onMouseLeave", function () {
      _this.isUserInteracting = false;

      for (var _toastId2 in _this.toastIdToTimerMap) {
        if (_this.toastIdToTimerMap.hasOwnProperty(_toastId2)) {
          var timer = _this.toastIdToTimerMap[_toastId2];
          timer.resume();
        }
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onScroll", function () {
      if (_this.listElement) {
        _this.isScrolledToBottom = _this.listElement.scrollHeight - _this.listElement.scrollTop === _this.listElement.clientHeight;
      }
    });

    _defineProperty(_assertThisInitialized(_this), "scheduleAllToastsForDismissal", function () {
      _this.props.toasts.forEach(function (toast) {
        if (!_this.toastIdToTimerMap[toast.id]) {
          _this.scheduleToastForDismissal(toast);
        }
      });
    });

    _defineProperty(_assertThisInitialized(_this), "scheduleToastForDismissal", function (toast) {
      // Start fading the toast out once its lifetime elapses.
      _this.toastIdToTimerMap[toast.id] = new Timer(_this.dismissToast.bind(_assertThisInitialized(_this), toast), toast.toastLifeTimeMs != null ? toast.toastLifeTimeMs : _this.props.toastLifeTimeMs);
    });

    _defineProperty(_assertThisInitialized(_this), "dismissToast", function (toast) {
      // Remove the toast after it's done fading out.
      _this.dismissTimeoutIds.push(window.setTimeout(function () {
        // Because this is wrapped in a setTimeout, and because React does not guarantee when
        // state updates happen, it is possible to double-dismiss a toast
        // including by double-clicking the "x" button on the toast
        // so, first check to make sure we haven't already dismissed this toast
        if (_this.toastIdToTimerMap.hasOwnProperty(toast.id)) {
          _this.props.dismissToast.apply(_assertThisInitialized(_this), [toast]);

          _this.toastIdToTimerMap[toast.id].clear();

          delete _this.toastIdToTimerMap[toast.id];

          _this.setState(function (prevState) {
            var toastIdToDismissedMap = _objectSpread({}, prevState.toastIdToDismissedMap);

            delete toastIdToDismissedMap[toast.id];
            return {
              toastIdToDismissedMap: toastIdToDismissedMap
            };
          });
        }
      }, TOAST_FADE_OUT_MS));

      _this.setState(function (prevState) {
        var toastIdToDismissedMap = _objectSpread(_objectSpread({}, prevState.toastIdToDismissedMap), {}, _defineProperty({}, toast.id, true));

        return {
          toastIdToDismissedMap: toastIdToDismissedMap
        };
      });
    });

    return _this;
  }

  _createClass(EuiGlobalToastList, [{
    key: "startScrollingToBottom",
    value: function startScrollingToBottom() {
      var _this2 = this;

      this.isScrollingToBottom = true;

      var scrollToBottom = function scrollToBottom() {
        // Although we cancel the requestAnimationFrame in componentWillUnmount,
        // it's possible for this.listElement to become null in the meantime
        if (!_this2.listElement) {
          return;
        }

        var position = _this2.listElement.scrollTop;
        var destination = _this2.listElement.scrollHeight - _this2.listElement.clientHeight;
        var distanceToDestination = destination - position;

        if (distanceToDestination < 5) {
          _this2.listElement.scrollTop = destination;
          _this2.isScrollingToBottom = false;
          _this2.isScrolledToBottom = true;
          return;
        }

        _this2.listElement.scrollTop = position + distanceToDestination * 0.25;

        if (_this2.isScrollingToBottom) {
          _this2.isScrollingAnimationFrame = window.requestAnimationFrame(scrollToBottom);
        }
      };

      this.startScrollingAnimationFrame = window.requestAnimationFrame(scrollToBottom);
    }
  }, {
    key: "componentDidMount",
    value: function componentDidMount() {
      if (this.listElement) {
        this.listElement.addEventListener('scroll', this.onScroll);
        this.listElement.addEventListener('mouseenter', this.onMouseEnter);
        this.listElement.addEventListener('mouseleave', this.onMouseLeave);
      }

      this.scheduleAllToastsForDismissal();
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      this.scheduleAllToastsForDismissal();

      if (!this.isUserInteracting) {
        // If the user has scrolled up the toast list then we don't want to annoy them by scrolling
        // all the way back to the bottom.
        if (this.isScrolledToBottom) {
          if (prevProps.toasts.length < this.props.toasts.length) {
            this.startScrollingToBottom();
          }
        }
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      if (this.isScrollingAnimationFrame !== 0) {
        window.cancelAnimationFrame(this.isScrollingAnimationFrame);
      }

      if (this.startScrollingAnimationFrame !== 0) {
        window.cancelAnimationFrame(this.startScrollingAnimationFrame);
      }

      if (this.listElement) {
        this.listElement.removeEventListener('scroll', this.onScroll);
        this.listElement.removeEventListener('mouseenter', this.onMouseEnter);
        this.listElement.removeEventListener('mouseleave', this.onMouseLeave);
      }

      this.dismissTimeoutIds.forEach(clearTimeout);

      for (var _toastId3 in this.toastIdToTimerMap) {
        if (this.toastIdToTimerMap.hasOwnProperty(_toastId3)) {
          var timer = this.toastIdToTimerMap[_toastId3];
          timer.clear();
        }
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this3 = this;

      var _this$props = this.props,
          className = _this$props.className,
          toasts = _this$props.toasts,
          dismissToast = _this$props.dismissToast,
          toastLifeTimeMs = _this$props.toastLifeTimeMs,
          side = _this$props.side,
          rest = _objectWithoutProperties(_this$props, ["className", "toasts", "dismissToast", "toastLifeTimeMs", "side"]);

      var renderedToasts = toasts.map(function (toast) {
        var text = toast.text,
            toastLifeTimeMs = toast.toastLifeTimeMs,
            rest = _objectWithoutProperties(toast, ["text", "toastLifeTimeMs"]);

        return ___EmotionJSX(EuiGlobalToastListItem, {
          key: toast.id,
          isDismissed: _this3.state.toastIdToDismissedMap[toast.id]
        }, ___EmotionJSX(EuiToast, _extends({
          onClose: _this3.dismissToast.bind(_this3, toast),
          onFocus: _this3.onMouseEnter,
          onBlur: _this3.onMouseLeave
        }, rest), text));
      });
      var classes = classNames('euiGlobalToastList', side ? sideToClassNameMap[side] : null, className);
      return ___EmotionJSX("div", _extends({
        "aria-live": "polite",
        role: "region",
        ref: function ref(element) {
          _this3.listElement = element;
        },
        className: classes
      }, rest), renderedToasts);
    }
  }]);

  return EuiGlobalToastList;
}(Component);

_defineProperty(EuiGlobalToastList, "defaultProps", {
  toasts: [],
  side: 'right'
});

EuiGlobalToastList.propTypes = {
  toasts: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.string.isRequired,
    text: PropTypes.node,
    toastLifeTimeMs: PropTypes.number,
    title: PropTypes.node,
    color: PropTypes.oneOf(["primary", "success", "warning", "danger"]),
    iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),
    onClose: PropTypes.func,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }).isRequired).isRequired,
  dismissToast: PropTypes.func.isRequired,
  toastLifeTimeMs: PropTypes.number.isRequired,

  /**
     * Determines which side of the browser window the toasts should appear
     */
  side: PropTypes.oneOf(["right", "left"]),
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};