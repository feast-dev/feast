import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { isValidElement } from 'react';
import classNames from 'classnames';
import { keysOf } from '../common';
import { getSecureRelForTarget } from '../../services';
import { EuiText } from '../text';
import { EuiTitle } from '../title';
import { EuiBetaBadge } from '../badge/beta_badge';
import { EuiCardSelect, euiCardSelectableColor } from './card_select';
import { useGeneratedHtmlId } from '../../services/accessibility';
import { validateHref } from '../../services/security/href_validator';
import { EuiPanel } from '../panel';
import { jsx as ___EmotionJSX } from "@emotion/react";
var textAlignToClassNameMap = {
  left: 'euiCard--leftAligned',
  center: 'euiCard--centerAligned',
  right: 'euiCard--rightAligned'
};
export var ALIGNMENTS = keysOf(textAlignToClassNameMap);
var layoutToClassNameMap = {
  vertical: '',
  horizontal: 'euiCard--horizontal'
};
export var LAYOUT_ALIGNMENTS = keysOf(layoutToClassNameMap);
/**
 * Certain props are only allowed when the layout is vertical
 */

export var EuiCard = function EuiCard(_ref) {
  var className = _ref.className,
      description = _ref.description,
      _isDisabled = _ref.isDisabled,
      title = _ref.title,
      _ref$titleElement = _ref.titleElement,
      titleElement = _ref$titleElement === void 0 ? 'span' : _ref$titleElement,
      _ref$titleSize = _ref.titleSize,
      titleSize = _ref$titleSize === void 0 ? 's' : _ref$titleSize,
      icon = _ref.icon,
      image = _ref.image,
      children = _ref.children,
      footer = _ref.footer,
      onClick = _ref.onClick,
      href = _ref.href,
      rel = _ref.rel,
      target = _ref.target,
      _ref$textAlign = _ref.textAlign,
      textAlign = _ref$textAlign === void 0 ? 'center' : _ref$textAlign,
      betaBadgeProps = _ref.betaBadgeProps,
      _ref$layout = _ref.layout,
      layout = _ref$layout === void 0 ? 'vertical' : _ref$layout,
      selectable = _ref.selectable,
      display = _ref.display,
      paddingSize = _ref.paddingSize,
      rest = _objectWithoutProperties(_ref, ["className", "description", "isDisabled", "title", "titleElement", "titleSize", "icon", "image", "children", "footer", "onClick", "href", "rel", "target", "textAlign", "betaBadgeProps", "layout", "selectable", "display", "paddingSize"]);

  var isHrefValid = !href || validateHref(href);
  var isDisabled = _isDisabled || !isHrefValid;
  var isClickable = !isDisabled && (onClick || href || selectable && !selectable.isDisabled);
  /**
   * For a11y, we simulate the same click that's provided on the title when clicking the whole card
   * without having to make the whole card a button or anchor tag.
   * *Card Accessibility: The redundant click event https://inclusive-components.design/cards/*
   */

  var link = null;

  var outerOnClick = function outerOnClick(e) {
    if (link && link !== e.target) {
      link.click();
    }
  };

  if (layout === 'horizontal') {
    if (image || footer || textAlign !== 'center') {
      throw new Error('EuiCard: `layout="horizontal"` cannot be used in conjunction with `image`, `footer`, or `textAlign`.');
    }
  }

  var selectableColorClass = selectable ? "euiCard--isSelectable--".concat(euiCardSelectableColor(selectable.color, selectable.isSelected)) : undefined;
  var classes = classNames('euiCard', textAlignToClassNameMap[textAlign], layoutToClassNameMap[layout], {
    'euiCard--isClickable': isClickable,
    'euiCard--hasBetaBadge': betaBadgeProps === null || betaBadgeProps === void 0 ? void 0 : betaBadgeProps.label,
    'euiCard--hasIcon': icon,
    'euiCard--isSelectable': selectable,
    'euiCard-isSelected': selectable === null || selectable === void 0 ? void 0 : selectable.isSelected,
    'euiCard-isDisabled': isDisabled
  }, selectableColorClass, className);
  var ariaId = useGeneratedHtmlId();
  var ariaDesc = description ? "".concat(ariaId, "Description") : '';
  /**
   * Top area containing image, icon or both
   */

  var imageNode;

  if (image && layout === 'vertical') {
    if ( /*#__PURE__*/isValidElement(image) || typeof image === 'string') {
      imageNode = ___EmotionJSX("div", {
        className: "euiCard__image"
      }, /*#__PURE__*/isValidElement(image) ? image : ___EmotionJSX("img", {
        src: image,
        alt: ""
      }));
    } else {
      imageNode = null;
    }
  }

  var iconNode;

  if (icon) {
    iconNode = /*#__PURE__*/React.cloneElement(icon, {
      className: classNames(icon.props.className, 'euiCard__icon')
    });
  }

  var optionalCardTop;

  if (imageNode || iconNode) {
    optionalCardTop = ___EmotionJSX("div", {
      className: "euiCard__top"
    }, imageNode, iconNode);
  }
  /**
   * Optional EuiBetaBadge
   */


  var optionalBetaBadge;
  var optionalBetaBadgeID = '';

  if (betaBadgeProps === null || betaBadgeProps === void 0 ? void 0 : betaBadgeProps.label) {
    optionalBetaBadgeID = "".concat(ariaId, "BetaBadge");
    optionalBetaBadge = ___EmotionJSX("span", {
      className: "euiCard__betaBadgeWrapper"
    }, ___EmotionJSX(EuiBetaBadge, _extends({
      id: optionalBetaBadgeID
    }, betaBadgeProps, {
      className: classNames('euiCard__betaBadge', betaBadgeProps === null || betaBadgeProps === void 0 ? void 0 : betaBadgeProps.className)
    }))); // Increase padding size when there is a beta badge unless it's already determined

    paddingSize = paddingSize || 'l';
  }
  /**
   * Optional selectable button
   */


  if (selectable && isDisabled && selectable.isDisabled === undefined) {
    selectable.isDisabled = isDisabled;
  }

  var optionalSelectButton;

  if (selectable) {
    optionalSelectButton = ___EmotionJSX(EuiCardSelect, _extends({
      "aria-describedby": "".concat(ariaId, "Title ").concat(ariaDesc)
    }, selectable, {
      buttonRef: function buttonRef(node) {
        link = node;
      }
    }));
  }
  /**
   * Wraps the title with the link (<a>) or button.
   * This makes the title element a11y friendly and gets described by its content if its interactable.
   */


  var theTitle;

  if (!isDisabled && href) {
    theTitle = ___EmotionJSX("a", {
      className: "euiCard__titleAnchor",
      onClick: onClick,
      href: href,
      target: target,
      "aria-describedby": ariaDesc,
      rel: getSecureRelForTarget({
        href: href,
        target: target,
        rel: rel
      }),
      ref: function ref(node) {
        link = node;
      }
    }, title);
  } else if (isDisabled || onClick) {
    theTitle = ___EmotionJSX("button", {
      className: "euiCard__titleButton",
      onClick: onClick,
      disabled: isDisabled,
      "aria-describedby": "".concat(optionalBetaBadgeID, " ").concat(ariaDesc),
      ref: function ref(node) {
        link = node;
      }
    }, title);
  } else {
    theTitle = title;
  }
  /**
   * Convert titleElement to a capital TitleElement
   */


  var TitleElement = titleElement;
  return ___EmotionJSX(EuiPanel, _extends({
    element: "div",
    className: classes,
    onClick: isClickable ? outerOnClick : undefined,
    color: isDisabled ? 'subdued' : display,
    hasShadow: isDisabled || display ? false : true,
    hasBorder: display ? false : undefined,
    paddingSize: paddingSize
  }, rest), optionalCardTop, ___EmotionJSX("div", {
    className: "euiCard__content"
  }, ___EmotionJSX(EuiTitle, {
    id: "".concat(ariaId, "Title"),
    className: "euiCard__title",
    size: titleSize
  }, ___EmotionJSX(TitleElement, null, theTitle)), description && ___EmotionJSX(EuiText, {
    id: ariaDesc,
    size: "s",
    className: "euiCard__description"
  }, ___EmotionJSX("p", null, description)), children && ___EmotionJSX("div", {
    className: "euiCard__children"
  }, children)), optionalBetaBadge, layout === 'vertical' && footer && ___EmotionJSX("div", {
    className: "euiCard__footer"
  }, footer), optionalSelectButton);
};