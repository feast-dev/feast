/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
export var makeRowManager = function makeRowManager(containerRef) {
  var rowIdToElements = new Map();
  return {
    getRow: function getRow(rowIndex) {
      var rowElement = rowIdToElements.get(rowIndex);

      if (rowElement == null) {
        var _containerRef$current;

        rowElement = document.createElement('div');
        rowElement.setAttribute('role', 'row');
        rowElement.classList.add('euiDataGridRow');
        rowIdToElements.set(rowIndex, rowElement); // add the element to the wrapping container

        (_containerRef$current = containerRef.current) === null || _containerRef$current === void 0 ? void 0 : _containerRef$current.appendChild(rowElement); // watch the row's children, if they all disappear then remove this row

        var observer = new MutationObserver(function (records) {
          if (records[0].target.childElementCount === 0) {
            var _rowElement;

            observer.disconnect();
            (_rowElement = rowElement) === null || _rowElement === void 0 ? void 0 : _rowElement.remove();
            rowIdToElements.delete(rowIndex);
          }
        });
        observer.observe(rowElement, {
          childList: true
        });
      }

      return rowElement;
    }
  };
};