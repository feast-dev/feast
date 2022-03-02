import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
export var POSITIONS = ['top', 'right', 'bottom', 'left'];
var relatedDimension = {
  top: 'height',
  right: 'width',
  bottom: 'height',
  left: 'width'
};
var dimensionPositionAttribute = {
  height: 'top',
  width: 'left'
};
var positionComplements = {
  top: 'bottom',
  right: 'left',
  bottom: 'top',
  left: 'right'
}; // always resolving to top/left is taken advantage of by knowing they are the
// minimum edges of the bounding box

var positionSubstitutes = {
  top: 'left',
  right: 'top',
  bottom: 'left',
  left: 'top'
};

var getBufferValues = function getBufferValues(buffer) {
  if (Array.isArray(buffer)) {
    var _buffer = _slicedToArray(buffer, 4),
        topBuffer = _buffer[0],
        rightBuffer = _buffer[1],
        bottomBuffer = _buffer[2],
        leftBuffer = _buffer[3];

    return [topBuffer, rightBuffer, bottomBuffer, leftBuffer];
  }

  return [buffer, buffer, buffer, buffer];
};
/**
 * Calculates the absolute positioning (relative to document.body) to place a popover element
 *
 * @param anchor {HTMLElement} Element to anchor the popover to
 * @param popover {HTMLElement} Element containing the popover content
 * @param position {string} Position the user wants. One of ["top", "right", "bottom", "left"]
 * @param [forcePosition] {boolean} If true, use only the provided `position` value and don't try any other position
 * @param [align] {string} Cross-axis alignment. One of ["top", "right", "bottom", "left"]
 * @param [buffer=16] {number} Minimum distance between the popover and the bounding container
 * @param [offset=0] {number} Distance between the popover and the anchor
 * @param [allowCrossAxis=true] {boolean} Whether to allow the popover to be positioned on the cross-axis
 * @param [container] {HTMLElement} Element the popover must be constrained to fit within
 * @param [arrowConfig] {{arrowWidth: number, arrowBuffer: number}} If
 *  present, describes the size & constraints for an arrow element, and the
 *  function return value will include an `arrow` param with position details
 *
 * @returns {FindPopoverPositionResult} absolute page coordinates for the
 * popover, and the placement's relation to the anchor or undefined
 * there's no room.
 */


export function findPopoverPosition(_ref) {
  var anchor = _ref.anchor,
      popover = _ref.popover,
      align = _ref.align,
      position = _ref.position,
      forcePosition = _ref.forcePosition,
      _ref$buffer = _ref.buffer,
      buffer = _ref$buffer === void 0 ? 16 : _ref$buffer,
      _ref$offset = _ref.offset,
      offset = _ref$offset === void 0 ? 0 : _ref$offset,
      _ref$allowCrossAxis = _ref.allowCrossAxis,
      allowCrossAxis = _ref$allowCrossAxis === void 0 ? true : _ref$allowCrossAxis,
      container = _ref.container,
      arrowConfig = _ref.arrowConfig,
      returnBoundingBox = _ref.returnBoundingBox;
  // find the screen-relative bounding boxes of the anchor, popover, and container
  var anchorBoundingBox = getElementBoundingBox(anchor);
  var popoverBoundingBox = getElementBoundingBox(popover); // calculate the window's bounds
  // window.(innerWidth|innerHeight) do not account for scrollbars
  // so prefer the clientWidth/clientHeight of the DOM if available

  var documentWidth = document.documentElement.clientWidth || window.innerWidth;
  var documentHeight = document.documentElement.clientHeight || window.innerHeight;
  var windowBoundingBox = {
    top: 0,
    right: documentWidth,
    bottom: documentHeight,
    left: 0,
    height: documentHeight,
    width: documentWidth
  }; // if no container element is given fall back to using the window viewport

  var containerBoundingBox = container ? getElementBoundingBox(container) : windowBoundingBox;
  /**
   * `position` was specified by the function caller and is a strong hint
   * as to the preferred location of the popover relative to the anchor.
   * However, we strongly prefer showing all of the popover content within
   * the window+container boundary and will iterate over the four
   * possible sides until a perfect fit is located. If none of the locations
   * fully contain popover, the location with the best fit is selected.
   *
   * This approach first checks the preferred `position`, then its opposite
   * along the same axis, next a location on the cross-axis, and finally it
   * tests the remaining position.
   *
   * e.g.
   * if position = "top" the order is top, bottom, left right
   * if position = "right" the order is right, left, top, bottom
   */
  // Try the user-desired position first.

  var iterationPositions = [position]; // keep user-defined alignment in the original positions.

  var iterationAlignments = [align];

  if (forcePosition !== true) {
    iterationPositions.push(positionComplements[position]); // Try the complementary position.

    iterationAlignments.push(align); // keep user-defined alignment in the complementary position.

    if (allowCrossAxis) {
      iterationPositions.push(positionSubstitutes[position], // Switch to the cross axis.
      positionComplements[positionSubstitutes[position]] // Try the complementary position on the cross axis.
      );
      iterationAlignments.push(undefined, undefined); // discard desired alignment on cross-axis
    }
  } else {
    // position is forced, if it conflicts with the alignment then reset align to `null`
    // e.g. original placement request for `downLeft` is moved to the `left` side, future calls
    // will position and align `left`, and `leftLeft` is not a valid placement
    if (position === align || align !== undefined && position === positionComplements[align]) {
      iterationAlignments[0] = undefined;
    }
  }

  var bestFit = undefined;
  var bestPosition = null;

  for (var idx = 0; idx < iterationPositions.length; idx++) {
    var iterationPosition = iterationPositions[idx]; // See if we can find a position with a better fit than we've found so far.

    var screenCoordinates = getPopoverScreenCoordinates({
      position: iterationPosition,
      align: iterationAlignments[idx],
      anchorBoundingBox: anchorBoundingBox,
      popoverBoundingBox: popoverBoundingBox,
      windowBoundingBox: windowBoundingBox,
      containerBoundingBox: containerBoundingBox,
      offset: offset,
      buffer: buffer,
      arrowConfig: arrowConfig
    });

    if (bestFit === undefined || screenCoordinates.fit > bestFit) {
      bestFit = screenCoordinates.fit;
      bestPosition = {
        fit: screenCoordinates.fit,
        position: iterationPosition,
        top: screenCoordinates.top + window.pageYOffset,
        left: screenCoordinates.left + window.pageXOffset,
        arrow: screenCoordinates.arrow
      }; // If we've already found the ideal fit, use that position.

      if (bestFit === 1) {
        break;
      }
    } // If we haven't improved the fit, then continue on and try a new position.

  }

  if (bestPosition == null) {
    throw new Error('Failed to calculate bestPosition');
  }

  if (returnBoundingBox) {
    bestPosition.anchorBoundingBox = anchorBoundingBox;
  }

  return bestPosition;
}

/**
 * Given a target position and the popover's surrounding context, returns either an
 * object with {top, left} screen coordinates or `null` if it's not possible to show
 * content in the target position
 * @param position {string} the target position, one of ["top", "right", "bottom", "left"]
 * @param align {string} target alignment on the cross-axis, one of ["top", "right", "bottom", "left"]
 * @param anchorBoundingBox {Object} bounding box of the anchor element
 * @param popoverBoundingBox {Object} bounding box of the popover element
 * @param windowBoundingBox {Object} bounding box of the window
 * @param containerBoundingBox {Object} bounding box of the container
 * @param [arrowConfig] {{arrowWidth: number, arrowBuffer: number}} If present, describes the size &
 *  constraints for an arrow element, and the function return value will include an `arrow` param
 *  with position details
 * @param [offset=0] {number} Distance between the popover and the anchor
 * @param [buffer=0] {number} Minimum distance between the popover's
 *  placement and the container edge
 *
 * @returns {GetPopoverScreenCoordinatesResult}
 *  object with top/left coordinates, the popover's relative position to the anchor, and how well the
 *  popover fits in the location (0.0 -> 1.0) coordinates and the popover's relative position, if
 *  there is no room in this placement then null
 */
export function getPopoverScreenCoordinates(_ref2) {
  var _popoverPlacement, _ref3;

  var position = _ref2.position,
      align = _ref2.align,
      anchorBoundingBox = _ref2.anchorBoundingBox,
      popoverBoundingBox = _ref2.popoverBoundingBox,
      windowBoundingBox = _ref2.windowBoundingBox,
      containerBoundingBox = _ref2.containerBoundingBox,
      arrowConfig = _ref2.arrowConfig,
      _ref2$offset = _ref2.offset,
      offset = _ref2$offset === void 0 ? 0 : _ref2$offset,
      _ref2$buffer = _ref2.buffer,
      buffer = _ref2$buffer === void 0 ? 0 : _ref2$buffer;

  /**
   * The goal is to find the best way to align the popover content
   * on the given side of the anchor element. The popover prefers
   * centering on the anchor but can shift along the cross-axis as needed.
   *
   * We return the top/left coordinates that best fit the popover inside
   * the given boundaries, and also return the `fit` value which indicates
   * what percentage of the popover is within the bounds.
   *
   * e.g. finding a location when position=top
   * the preferred location is directly over the anchor
   *
   *        +----------------------+
   *        |       popover        |
   *        +----------------------+
   *                   v
   *            +--------------+
   *            |    anchor    |
   *            +--------------+
   *
   * but if anchor doesn't have much (or any) room on its ride side
   * the popover will shift to the left
   *
   *    +----------------------+
   *    |       popover        |
   *    +----------------------+
   *                   v
   *            +--------------+
   *            |    anchor    |
   *            +--------------+
   *
   */
  var crossAxisFirstSide = positionSubstitutes[position]; // "top" -> "left"

  var crossAxisSecondSide = positionComplements[crossAxisFirstSide]; // "left" -> "right"

  var crossAxisDimension = relatedDimension[crossAxisFirstSide]; // "left" -> "width"

  var _getBufferValues = getBufferValues(buffer),
      _getBufferValues2 = _slicedToArray(_getBufferValues, 4),
      topBuffer = _getBufferValues2[0],
      rightBuffer = _getBufferValues2[1],
      bottomBuffer = _getBufferValues2[2],
      leftBuffer = _getBufferValues2[3];

  var _getCrossAxisPosition = getCrossAxisPosition({
    crossAxisFirstSide: crossAxisFirstSide,
    crossAxisSecondSide: crossAxisSecondSide,
    crossAxisDimension: crossAxisDimension,
    position: position,
    align: align,
    buffer: buffer,
    offset: offset,
    windowBoundingBox: windowBoundingBox,
    containerBoundingBox: containerBoundingBox,
    popoverBoundingBox: popoverBoundingBox,
    anchorBoundingBox: anchorBoundingBox,
    arrowConfig: arrowConfig
  }),
      crossAxisPosition = _getCrossAxisPosition.crossAxisPosition,
      crossAxisArrowPosition = _getCrossAxisPosition.crossAxisArrowPosition;

  var primaryAxisDimension = relatedDimension[position]; // "top" -> "height"

  var primaryAxisPositionName = dimensionPositionAttribute[primaryAxisDimension]; // "height" -> "top"

  var _getPrimaryAxisPositi = getPrimaryAxisPosition({
    position: position,
    offset: offset,
    popoverBoundingBox: popoverBoundingBox,
    anchorBoundingBox: anchorBoundingBox,
    arrowConfig: arrowConfig
  }),
      primaryAxisPosition = _getPrimaryAxisPositi.primaryAxisPosition,
      primaryAxisArrowPosition = _getPrimaryAxisPositi.primaryAxisArrowPosition;

  var popoverPlacement = (_popoverPlacement = {}, _defineProperty(_popoverPlacement, crossAxisFirstSide, crossAxisPosition), _defineProperty(_popoverPlacement, primaryAxisPositionName, primaryAxisPosition), _popoverPlacement); // calculate the fit of the popover in this location
  // fit is in range 0.0 -> 1.0 and is the percentage of the popover which is visible in this location

  var combinedBoundingBox = intersectBoundingBoxes(windowBoundingBox, containerBoundingBox); // shrink the visible bounding box by `buffer`
  // to compute a fit value

  combinedBoundingBox.top += topBuffer;
  combinedBoundingBox.right -= rightBuffer;
  combinedBoundingBox.bottom -= bottomBuffer;
  combinedBoundingBox.left += leftBuffer;
  var fit = getVisibleFit({
    top: popoverPlacement.top,
    right: popoverPlacement.left + popoverBoundingBox.width,
    bottom: popoverPlacement.top + popoverBoundingBox.height,
    left: popoverPlacement.left,
    width: popoverBoundingBox.width,
    height: popoverBoundingBox.height
  }, combinedBoundingBox);
  var arrow = arrowConfig ? (_ref3 = {}, _defineProperty(_ref3, crossAxisFirstSide, crossAxisArrowPosition - popoverPlacement[crossAxisFirstSide]), _defineProperty(_ref3, primaryAxisPositionName, primaryAxisArrowPosition), _ref3) : undefined;
  return {
    fit: fit,
    top: popoverPlacement.top,
    left: popoverPlacement.left,
    arrow: arrow ? {
      left: arrow.left,
      top: arrow.top
    } : undefined
  };
}

function getCrossAxisPosition(_ref4) {
  var crossAxisFirstSide = _ref4.crossAxisFirstSide,
      crossAxisSecondSide = _ref4.crossAxisSecondSide,
      crossAxisDimension = _ref4.crossAxisDimension,
      position = _ref4.position,
      align = _ref4.align,
      buffer = _ref4.buffer,
      offset = _ref4.offset,
      windowBoundingBox = _ref4.windowBoundingBox,
      containerBoundingBox = _ref4.containerBoundingBox,
      popoverBoundingBox = _ref4.popoverBoundingBox,
      anchorBoundingBox = _ref4.anchorBoundingBox,
      arrowConfig = _ref4.arrowConfig;
  // how much of the popover overflows past either side of the anchor if its centered
  var popoverSizeOnCrossAxis = popoverBoundingBox[crossAxisDimension];
  var anchorSizeOnCrossAxis = anchorBoundingBox[crossAxisDimension];
  var anchorHalfSize = anchorSizeOnCrossAxis / 2; // the popover's original position on the cross-axis is determined by:

  var crossAxisPositionOriginal = anchorBoundingBox[crossAxisFirstSide] + // where the anchor is located
  anchorHalfSize - // plus half anchor dimension
  popoverSizeOnCrossAxis / 2; // less half the popover dimension
  // To fit the content within both the window and container,
  // compute the smaller of the two spaces along each edge

  var combinedBoundingBox = intersectBoundingBoxes(windowBoundingBox, containerBoundingBox);
  var availableSpace = getAvailableSpace(anchorBoundingBox, combinedBoundingBox, buffer, offset, position);
  var minimumSpace = arrowConfig ? arrowConfig.arrowBuffer : 0;
  var contentOverflowSize = (popoverSizeOnCrossAxis - anchorSizeOnCrossAxis) / 2;
  var alignAmount = 0;
  var alignDirection = 1;
  var amountOfShiftNeeded = 0;
  var shiftDirection = 1;

  if (align != null) {
    // no alignment, find how much the container boundary requires the content to shift
    alignDirection = align === 'top' || align === 'left' ? 1 : -1;
    alignAmount = contentOverflowSize;
    var alignedOverflowAmount = contentOverflowSize + alignAmount;
    var needsShift = alignedOverflowAmount > availableSpace[positionComplements[align]];
    amountOfShiftNeeded = needsShift ? alignedOverflowAmount - availableSpace[positionComplements[align]] : 0;
    shiftDirection = -1 * alignDirection;
  } else {
    // shifting the popover to one side may yield a better fit
    var spaceAvailableOnFirstSide = availableSpace[crossAxisFirstSide];
    var spaceAvailableOnSecondSide = availableSpace[crossAxisSecondSide];
    var isShiftTowardFirstSide = spaceAvailableOnFirstSide > spaceAvailableOnSecondSide;
    shiftDirection = isShiftTowardFirstSide ? -1 : 1; // determine which direction has more room and the popover should shift to

    var leastAvailableSpace = Math.min(spaceAvailableOnFirstSide, spaceAvailableOnSecondSide);

    var _needsShift = contentOverflowSize > leastAvailableSpace;

    amountOfShiftNeeded = _needsShift ? contentOverflowSize - leastAvailableSpace : 0;
  } // shift over the popover if necessary


  var shiftAmount = amountOfShiftNeeded * shiftDirection;
  var crossAxisPosition = crossAxisPositionOriginal + shiftAmount + alignAmount * alignDirection; // if an `arrowConfig` is specified, find where to position the arrow

  var crossAxisArrowPosition;

  if (arrowConfig) {
    var arrowWidth = arrowConfig.arrowWidth;
    crossAxisArrowPosition = anchorBoundingBox[crossAxisFirstSide] + anchorHalfSize - arrowWidth / 2; // make sure there's enough buffer around the arrow
    // by calculating how how much the arrow would need to move
    // but instead of moving the arrow, shift the popover content

    if (crossAxisArrowPosition < crossAxisPosition + minimumSpace) {
      // arrow is too close to the minimum side
      var difference = crossAxisPosition + minimumSpace - crossAxisArrowPosition;
      crossAxisPosition -= difference;
    } else if (crossAxisArrowPosition + minimumSpace + arrowWidth > crossAxisPosition + popoverSizeOnCrossAxis) {
      // arrow is too close to the maximum side
      var edge = crossAxisPosition + popoverSizeOnCrossAxis;

      var _difference = crossAxisArrowPosition - (edge - minimumSpace - arrowWidth);

      crossAxisPosition += _difference;
    }
  }

  return {
    crossAxisPosition: crossAxisPosition,
    crossAxisArrowPosition: crossAxisArrowPosition
  };
}

function getPrimaryAxisPosition(_ref5) {
  var position = _ref5.position,
      offset = _ref5.offset,
      popoverBoundingBox = _ref5.popoverBoundingBox,
      anchorBoundingBox = _ref5.anchorBoundingBox,
      arrowConfig = _ref5.arrowConfig;
  // if positioning to the top or left, the target position decreases
  // from the anchor's top or left, otherwise the position adds to the anchor's
  var isOffsetDecreasing = position === 'top' || position === 'left';
  var primaryAxisDimension = relatedDimension[position]; // "top" -> "height"

  var popoverSizeOnPrimaryAxis = popoverBoundingBox[primaryAxisDimension]; // start at the top or left edge of the anchor element

  var primaryAxisPositionName = dimensionPositionAttribute[primaryAxisDimension]; // "height" -> "top"

  var anchorEdgeOrigin = anchorBoundingBox[primaryAxisPositionName]; // find the popover position on the primary axis

  var anchorSizeOnPrimaryAxis = anchorBoundingBox[primaryAxisDimension];
  var primaryAxisOffset = isOffsetDecreasing ? popoverSizeOnPrimaryAxis : anchorSizeOnPrimaryAxis;
  var contentOffset = (offset + primaryAxisOffset) * (isOffsetDecreasing ? -1 : 1);
  var primaryAxisPosition = anchorEdgeOrigin + contentOffset;
  var primaryAxisArrowPosition;

  if (arrowConfig) {
    primaryAxisArrowPosition = isOffsetDecreasing ? popoverSizeOnPrimaryAxis : 0;
  }

  return {
    primaryAxisPosition: primaryAxisPosition,
    primaryAxisArrowPosition: primaryAxisArrowPosition
  };
}
/**
 * Finds the client pixel coordinate of each edge for the element's bounding box,
 * and the bounding box's width & height
 *
 * @param {HTMLElement} element
 * @returns {{top: number, right: number, bottom: number, left: number, height: number, width: number}}
 */


export function getElementBoundingBox(element) {
  var rect = element.getBoundingClientRect();
  return {
    top: rect.top,
    right: rect.right,
    bottom: rect.bottom,
    left: rect.left,
    height: rect.height,
    width: rect.width
  };
}
/**
 * Calculates the available content space between anchor and container
 *
 * @param {Object} anchorBoundingBox Client bounding box of the anchor element
 * @param {Object} containerBoundingBox Client bounding box of the container element
 * @param {number} buffer Minimum distance between the popover and the bounding container
 * @param {number} offset Distance between the popover and the anchor
 * @param {string} offsetSide Side the offset needs to be applied to, one
 *  of ["top", "right", "bottom", "left"]
 * @returns {{top: number, right: number, bottom: number, left: number}}
 */

export function getAvailableSpace(anchorBoundingBox, containerBoundingBox, buffer, offset, offsetSide) {
  var _getBufferValues3 = getBufferValues(buffer),
      _getBufferValues4 = _slicedToArray(_getBufferValues3, 4),
      topBuffer = _getBufferValues4[0],
      rightBuffer = _getBufferValues4[1],
      bottomBuffer = _getBufferValues4[2],
      leftBuffer = _getBufferValues4[3];

  return {
    top: anchorBoundingBox.top - containerBoundingBox.top - topBuffer - (offsetSide === 'top' ? offset : 0),
    right: containerBoundingBox.right - anchorBoundingBox.right - rightBuffer - (offsetSide === 'right' ? offset : 0),
    bottom: containerBoundingBox.bottom - anchorBoundingBox.bottom - bottomBuffer - (offsetSide === 'bottom' ? offset : 0),
    left: anchorBoundingBox.left - containerBoundingBox.left - leftBuffer - (offsetSide === 'left' ? offset : 0)
  };
}
/**
 * Computes the fit (overlap) of the content within the container, fit is in range 0.0 => 1.0
 * @param contentBoundingBox bounding box of content to calculate fit for
 * @param containerBoundingBox bounding box of container
 * @returns {number}
 */

export function getVisibleFit(contentBoundingBox, containerBoundingBox) {
  var intersection = intersectBoundingBoxes(contentBoundingBox, containerBoundingBox);

  if (intersection.left > intersection.right || intersection.top > intersection.top) {
    // there is no intersection, the boxes are completely separated on at least one axis
    return 0;
  }

  var intersectionArea = (intersection.right - intersection.left) * (intersection.bottom - intersection.top);
  var contentArea = (contentBoundingBox.right - contentBoundingBox.left) * (contentBoundingBox.bottom - contentBoundingBox.top);
  return intersectionArea / contentArea;
}
/**
 * Calculates the intersection space between two bounding boxes
 *
 * @param firstBox
 * @param secondBox
 * @returns {EuiClientRect}
 */

export function intersectBoundingBoxes(firstBox, secondBox) {
  var top = Math.max(firstBox.top, secondBox.top);
  var right = Math.min(firstBox.right, secondBox.right);
  var bottom = Math.min(firstBox.bottom, secondBox.bottom);
  var left = Math.max(firstBox.left, secondBox.left);
  var height = Math.max(bottom - top, 0);
  var width = Math.max(right - left, 0);
  return {
    top: top,
    right: right,
    bottom: bottom,
    left: left,
    height: height,
    width: width
  };
}
/**
 * Returns the top-most defined z-index in the element's ancestor hierarchy
 * relative to the `target` element; if no z-index is defined, returns 0
 * @param element {HTMLElement}
 * @param cousin {HTMLElement}
 * @returns {number}
 */

export function getElementZIndex(element, cousin) {
  /**
   * finding the z-index of `element` is not the full story
   * its the CSS stacking context that is important
   * take this DOM for example:
   * body
   *   section[z-index: 1000]
   *     p[z-index: 500]
   *       button
   *   div
   *
   * what z-index does the `div` need to display next to `button`?
   * the `div` and `section` are where the stacking context splits
   * so `div` needs to copy `section`'s z-index in order to
   * appear next to / over `button`
   *
   * calculate this by starting at `button` and finding its offsetParents
   * then walk the parents from top -> down until the stacking context
   * split is found, or if there is no split then a specific z-index is unimportant
   */
  // build the array of the element + its offset parents
  var nodesToInspect = [];

  while (true) {
    nodesToInspect.push(element); // AFAICT this is a valid cast - the libdefs appear wrong

    element = element.offsetParent; // stop if there is no parent

    if (element == null) {
      break;
    } // stop if the parent contains the related element
    // as this is the z-index ancestor


    if (element.contains(cousin)) {
      break;
    }
  } // reverse the nodes to walk from top -> element


  nodesToInspect.reverse();

  for (var _i = 0, _nodesToInspect = nodesToInspect; _i < _nodesToInspect.length; _i++) {
    var node = _nodesToInspect[_i];
    // get this node's z-index css value
    var zIndex = window.document.defaultView.getComputedStyle(node).getPropertyValue('z-index'); // if the z-index is not a number (e.g. "auto") return null, else the value

    var parsedZIndex = parseInt(zIndex, 10);

    if (!isNaN(parsedZIndex)) {
      return parsedZIndex;
    }
  }

  return 0;
}