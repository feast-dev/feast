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
import { EuiAccordion } from '../accordion';
import { useGeneratedHtmlId } from '../../services';
import { useEuiI18n } from '../i18n';
import { EuiText } from '../text';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiNotificationEventMessages = function EuiNotificationEventMessages(_ref) {
  var messages = _ref.messages,
      eventName = _ref.eventName;

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      isOpen = _useState2[0],
      setIsOpen = _useState2[1];

  var messagesLength = messages.length;
  var accordionId = useGeneratedHtmlId({
    prefix: 'euiNotificationEventMessagesAccordion'
  });
  var accordionButtonText = useEuiI18n('euiNotificationEventMessages.accordionButtonText', '+ {messagesLength} more', {
    messagesLength: messagesLength - 1
  });
  var accordionAriaLabelButtonText = useEuiI18n('euiNotificationEventMessages.accordionAriaLabelButtonText', '+ {messagesLength} messages for {eventName}', {
    messagesLength: messagesLength - 1,
    eventName: eventName
  });
  var accordionHideText = useEuiI18n('euiNotificationEventMessages.accordionHideText', 'hide');
  var buttonContentText = isOpen ? "".concat(accordionButtonText, " (").concat(accordionHideText, ")") : accordionButtonText;
  return ___EmotionJSX("div", {
    className: "euiNotificationEventMessages"
  }, messages && messagesLength === 1 ? ___EmotionJSX(EuiText, {
    size: "s",
    color: "subdued"
  }, ___EmotionJSX("p", null, messages)) : ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiText, {
    size: "s",
    color: "subdued"
  }, ___EmotionJSX("p", null, messages[0])), ___EmotionJSX(EuiAccordion, {
    onToggle: setIsOpen,
    buttonProps: {
      'aria-label': accordionAriaLabelButtonText
    },
    id: accordionId,
    className: "euiNotificationEventMessages__accordion",
    buttonContent: buttonContentText,
    buttonClassName: "euiNotificationEventMessages__accordionButton",
    arrowDisplay: "none"
  }, ___EmotionJSX("div", {
    className: "euiNotificationEventMessages__accordionContent"
  }, messages.map(function (notification, index) {
    return ___EmotionJSX(EuiText, {
      size: "s",
      key: index,
      color: "subdued"
    }, ___EmotionJSX("p", null, notification));
  }).slice(1)))));
};
EuiNotificationEventMessages.propTypes = {
  /*
     * An array of strings that get individually wrapped in `<p>` tags
     */
  messages: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,

  /**
     * A unique, human-friendly name for the event to be used in aria attributes (e.g. "alert-critical-01", "cloud-no-severity-12", etc..).
     */
  eventName: PropTypes.string.isRequired
};