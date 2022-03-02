import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useState } from 'react';
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