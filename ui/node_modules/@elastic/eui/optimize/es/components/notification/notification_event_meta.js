import _extends from "@babel/runtime/helpers/extends";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useState } from 'react';
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