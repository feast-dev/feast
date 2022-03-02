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
import { EuiButtonIcon } from '../button';
import { useEuiI18n } from '../i18n';
import classNames from 'classnames';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiNotificationEventReadButton = function EuiNotificationEventReadButton(_ref) {
  var id = _ref.id,
      isRead = _ref.isRead,
      onClick = _ref.onClick,
      eventName = _ref.eventName,
      rest = _objectWithoutProperties(_ref, ["id", "isRead", "onClick", "eventName"]);

  var classesReadState = classNames('euiNotificationEventReadButton', {
    'euiNotificationEventReadButton--isRead': isRead
  });
  var markAsReadAria = useEuiI18n('euiNotificationEventReadButton.markAsReadAria', 'Mark {eventName} as read', {
    eventName: eventName
  });
  var markAsUnreadAria = useEuiI18n('euiNotificationEventReadButton.markAsUnreadAria', 'Mark {eventName} as unread', {
    eventName: eventName
  });
  var markAsRead = useEuiI18n('euiNotificationEventReadButton.markAsRead', 'Mark as read');
  var markAsUnread = useEuiI18n('euiNotificationEventReadButton.markAsUnread', 'Mark as unread');
  var buttonAriaLabel = isRead ? markAsUnreadAria : markAsReadAria;
  var buttonTitle = isRead ? markAsUnread : markAsRead;
  return ___EmotionJSX(EuiButtonIcon, _extends({
    iconType: "dot",
    "aria-label": buttonAriaLabel,
    title: buttonTitle,
    className: classesReadState,
    onClick: onClick,
    "data-test-subj": "".concat(id, "-notificationEventReadButton")
  }, rest));
};
EuiNotificationEventReadButton.propTypes = {
  /**
     * Any of the named color palette options.
     */
  color: PropTypes.oneOf(["primary", "accent", "success", "warning", "danger", "ghost", "text"]),
  "aria-label": PropTypes.string,
  "aria-labelledby": PropTypes.string,

  /**
     * Size of the icon only.
     * This will not affect the overall size of the button
     */
  iconSize: PropTypes.oneOf(["original", "s", "m", "l", "xl", "xxl"]),

  /**
     * Sets the display style for matching other EuiButton types.
     * `base` is equivalent to a typical EuiButton
     * `fill` is equivalent to a filled EuiButton
     * `empty` (default) is equivalent to an EuiButtonEmpty
     */
  display: PropTypes.oneOf(["base", "empty", "fill"]),
  className: PropTypes.string,
  "data-test-subj": PropTypes.string,
  id: PropTypes.string.isRequired,

  /**
     * Shows an indicator of the read state of the event
     */
  isRead: PropTypes.bool.isRequired,

  /**
     * Applies an `onClick` handler to the `read` indicator.
     */
  onClick: PropTypes.func.isRequired,

  /**
     * A unique, human-friendly name for the event to be used in aria attributes (e.g. "alert-critical-01", "cloud-no-severity-12", etc..).
     */
  eventName: PropTypes.string.isRequired
};