import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutPropertiesLoose from "@babel/runtime/helpers/esm/objectWithoutPropertiesLoose";
import React from 'react';
import { useQueryClient } from 'react-query';
import { matchSorter } from 'match-sorter';
import useLocalStorage from './useLocalStorage';
import { useIsMounted, useSafeState } from './utils';
import { Panel, QueryKeys, QueryKey, Button, Code, Input, Select, ActiveQueryPanel } from './styledComponents';
import { ThemeProvider, defaultTheme as theme } from './theme';
import { getQueryStatusLabel, getQueryStatusColor } from './utils';
import Explorer from './Explorer';
import Logo from './Logo';
import { noop } from '../core/utils';
var isServer = typeof window === 'undefined';
export function ReactQueryDevtools(_ref) {
  var initialIsOpen = _ref.initialIsOpen,
      _ref$panelProps = _ref.panelProps,
      panelProps = _ref$panelProps === void 0 ? {} : _ref$panelProps,
      _ref$closeButtonProps = _ref.closeButtonProps,
      closeButtonProps = _ref$closeButtonProps === void 0 ? {} : _ref$closeButtonProps,
      _ref$toggleButtonProp = _ref.toggleButtonProps,
      toggleButtonProps = _ref$toggleButtonProp === void 0 ? {} : _ref$toggleButtonProp,
      _ref$position = _ref.position,
      position = _ref$position === void 0 ? 'bottom-left' : _ref$position,
      _ref$containerElement = _ref.containerElement,
      Container = _ref$containerElement === void 0 ? 'aside' : _ref$containerElement;
  var rootRef = React.useRef(null);
  var panelRef = React.useRef(null);

  var _useLocalStorage = useLocalStorage('reactQueryDevtoolsOpen', initialIsOpen),
      isOpen = _useLocalStorage[0],
      setIsOpen = _useLocalStorage[1];

  var _useLocalStorage2 = useLocalStorage('reactQueryDevtoolsHeight', null),
      devtoolsHeight = _useLocalStorage2[0],
      setDevtoolsHeight = _useLocalStorage2[1];

  var _useSafeState = useSafeState(false),
      isResolvedOpen = _useSafeState[0],
      setIsResolvedOpen = _useSafeState[1];

  var _useSafeState2 = useSafeState(false),
      isResizing = _useSafeState2[0],
      setIsResizing = _useSafeState2[1];

  var isMounted = useIsMounted();

  var _handleDragStart = function handleDragStart(panelElement, startEvent) {
    var _panelElement$getBoun;

    if (startEvent.button !== 0) return; // Only allow left click for drag

    setIsResizing(true);
    var dragInfo = {
      originalHeight: (_panelElement$getBoun = panelElement == null ? void 0 : panelElement.getBoundingClientRect().height) != null ? _panelElement$getBoun : 0,
      pageY: startEvent.pageY
    };

    var run = function run(moveEvent) {
      var delta = dragInfo.pageY - moveEvent.pageY;
      var newHeight = (dragInfo == null ? void 0 : dragInfo.originalHeight) + delta;
      setDevtoolsHeight(newHeight);

      if (newHeight < 70) {
        setIsOpen(false);
      } else {
        setIsOpen(true);
      }
    };

    var unsub = function unsub() {
      setIsResizing(false);
      document.removeEventListener('mousemove', run);
      document.removeEventListener('mouseUp', unsub);
    };

    document.addEventListener('mousemove', run);
    document.addEventListener('mouseup', unsub);
  };

  React.useEffect(function () {
    setIsResolvedOpen(isOpen != null ? isOpen : false);
  }, [isOpen, isResolvedOpen, setIsResolvedOpen]); // Toggle panel visibility before/after transition (depending on direction).
  // Prevents focusing in a closed panel.

  React.useEffect(function () {
    var ref = panelRef.current;

    if (ref) {
      var handlePanelTransitionStart = function handlePanelTransitionStart() {
        if (ref && isResolvedOpen) {
          ref.style.visibility = 'visible';
        }
      };

      var handlePanelTransitionEnd = function handlePanelTransitionEnd() {
        if (ref && !isResolvedOpen) {
          ref.style.visibility = 'hidden';
        }
      };

      ref.addEventListener('transitionstart', handlePanelTransitionStart);
      ref.addEventListener('transitionend', handlePanelTransitionEnd);
      return function () {
        ref.removeEventListener('transitionstart', handlePanelTransitionStart);
        ref.removeEventListener('transitionend', handlePanelTransitionEnd);
      };
    }
  }, [isResolvedOpen]);
  React[isServer ? 'useEffect' : 'useLayoutEffect'](function () {
    if (isResolvedOpen) {
      var _rootRef$current, _rootRef$current$pare;

      var previousValue = (_rootRef$current = rootRef.current) == null ? void 0 : (_rootRef$current$pare = _rootRef$current.parentElement) == null ? void 0 : _rootRef$current$pare.style.paddingBottom;

      var run = function run() {
        var _panelRef$current, _rootRef$current2;

        var containerHeight = (_panelRef$current = panelRef.current) == null ? void 0 : _panelRef$current.getBoundingClientRect().height;

        if ((_rootRef$current2 = rootRef.current) == null ? void 0 : _rootRef$current2.parentElement) {
          rootRef.current.parentElement.style.paddingBottom = containerHeight + "px";
        }
      };

      run();

      if (typeof window !== 'undefined') {
        window.addEventListener('resize', run);
        return function () {
          var _rootRef$current3;

          window.removeEventListener('resize', run);

          if (((_rootRef$current3 = rootRef.current) == null ? void 0 : _rootRef$current3.parentElement) && typeof previousValue === 'string') {
            rootRef.current.parentElement.style.paddingBottom = previousValue;
          }
        };
      }
    }
  }, [isResolvedOpen]);

  var _panelProps$style = panelProps.style,
      panelStyle = _panelProps$style === void 0 ? {} : _panelProps$style,
      otherPanelProps = _objectWithoutPropertiesLoose(panelProps, ["style"]);

  var _closeButtonProps$sty = closeButtonProps.style,
      closeButtonStyle = _closeButtonProps$sty === void 0 ? {} : _closeButtonProps$sty,
      onCloseClick = closeButtonProps.onClick,
      otherCloseButtonProps = _objectWithoutPropertiesLoose(closeButtonProps, ["style", "onClick"]);

  var _toggleButtonProps$st = toggleButtonProps.style,
      toggleButtonStyle = _toggleButtonProps$st === void 0 ? {} : _toggleButtonProps$st,
      onToggleClick = toggleButtonProps.onClick,
      otherToggleButtonProps = _objectWithoutPropertiesLoose(toggleButtonProps, ["style", "onClick"]); // Do not render on the server


  if (!isMounted()) return null;
  return /*#__PURE__*/React.createElement(Container, {
    ref: rootRef,
    className: "ReactQueryDevtools",
    "aria-label": "React Query Devtools"
  }, /*#__PURE__*/React.createElement(ThemeProvider, {
    theme: theme
  }, /*#__PURE__*/React.createElement(ReactQueryDevtoolsPanel, _extends({
    ref: panelRef
  }, otherPanelProps, {
    style: _extends({
      position: 'fixed',
      bottom: '0',
      right: '0',
      zIndex: 99999,
      width: '100%',
      height: devtoolsHeight != null ? devtoolsHeight : 500,
      maxHeight: '90%',
      boxShadow: '0 0 20px rgba(0,0,0,.3)',
      borderTop: "1px solid " + theme.gray,
      transformOrigin: 'top',
      // visibility will be toggled after transitions, but set initial state here
      visibility: isOpen ? 'visible' : 'hidden'
    }, panelStyle, isResizing ? {
      transition: "none"
    } : {
      transition: "all .2s ease"
    }, isResolvedOpen ? {
      opacity: 1,
      pointerEvents: 'all',
      transform: "translateY(0) scale(1)"
    } : {
      opacity: 0,
      pointerEvents: 'none',
      transform: "translateY(15px) scale(1.02)"
    }),
    isOpen: isResolvedOpen,
    setIsOpen: setIsOpen,
    handleDragStart: function handleDragStart(e) {
      return _handleDragStart(panelRef.current, e);
    }
  })), isResolvedOpen ? /*#__PURE__*/React.createElement(Button, _extends({
    type: "button",
    "aria-label": "Close React Query Devtools",
    "aria-controls": "ReactQueryDevtoolsPanel",
    "aria-haspopup": "true",
    "aria-expanded": "true"
  }, otherCloseButtonProps, {
    onClick: function onClick(e) {
      setIsOpen(false);
      onCloseClick && onCloseClick(e);
    },
    style: _extends({
      position: 'fixed',
      zIndex: 99999,
      margin: '.5em',
      bottom: 0
    }, position === 'top-right' ? {
      right: '0'
    } : position === 'top-left' ? {
      left: '0'
    } : position === 'bottom-right' ? {
      right: '0'
    } : {
      left: '0'
    }, closeButtonStyle)
  }), "Close") : null), !isResolvedOpen ? /*#__PURE__*/React.createElement("button", _extends({
    type: "button"
  }, otherToggleButtonProps, {
    "aria-label": "Open React Query Devtools",
    "aria-controls": "ReactQueryDevtoolsPanel",
    "aria-haspopup": "true",
    "aria-expanded": "false",
    onClick: function onClick(e) {
      setIsOpen(true);
      onToggleClick && onToggleClick(e);
    },
    style: _extends({
      background: 'none',
      border: 0,
      padding: 0,
      position: 'fixed',
      zIndex: 99999,
      display: 'inline-flex',
      fontSize: '1.5em',
      margin: '.5em',
      cursor: 'pointer',
      width: 'fit-content'
    }, position === 'top-right' ? {
      top: '0',
      right: '0'
    } : position === 'top-left' ? {
      top: '0',
      left: '0'
    } : position === 'bottom-right' ? {
      bottom: '0',
      right: '0'
    } : {
      bottom: '0',
      left: '0'
    }, toggleButtonStyle)
  }), /*#__PURE__*/React.createElement(Logo, {
    "aria-hidden": true
  })) : null);
}

var getStatusRank = function getStatusRank(q) {
  return q.state.isFetching ? 0 : !q.getObserversCount() ? 3 : q.isStale() ? 2 : 1;
};

var sortFns = {
  'Status > Last Updated': function StatusLastUpdated(a, b) {
    var _sortFns$LastUpdated;

    return getStatusRank(a) === getStatusRank(b) ? (_sortFns$LastUpdated = sortFns['Last Updated']) == null ? void 0 : _sortFns$LastUpdated.call(sortFns, a, b) : getStatusRank(a) > getStatusRank(b) ? 1 : -1;
  },
  'Query Hash': function QueryHash(a, b) {
    return a.queryHash > b.queryHash ? 1 : -1;
  },
  'Last Updated': function LastUpdated(a, b) {
    return a.state.dataUpdatedAt < b.state.dataUpdatedAt ? 1 : -1;
  }
};
export var ReactQueryDevtoolsPanel = /*#__PURE__*/React.forwardRef(function ReactQueryDevtoolsPanel(props, ref) {
  var _activeQuery$state;

  var _props$isOpen = props.isOpen,
      isOpen = _props$isOpen === void 0 ? true : _props$isOpen,
      setIsOpen = props.setIsOpen,
      handleDragStart = props.handleDragStart,
      panelProps = _objectWithoutPropertiesLoose(props, ["isOpen", "setIsOpen", "handleDragStart"]);

  var queryClient = useQueryClient();
  var queryCache = queryClient.getQueryCache();

  var _useLocalStorage3 = useLocalStorage('reactQueryDevtoolsSortFn', Object.keys(sortFns)[0]),
      sort = _useLocalStorage3[0],
      setSort = _useLocalStorage3[1];

  var _useLocalStorage4 = useLocalStorage('reactQueryDevtoolsFilter', ''),
      filter = _useLocalStorage4[0],
      setFilter = _useLocalStorage4[1];

  var _useLocalStorage5 = useLocalStorage('reactQueryDevtoolsSortDesc', false),
      sortDesc = _useLocalStorage5[0],
      setSortDesc = _useLocalStorage5[1];

  var sortFn = React.useMemo(function () {
    return sortFns[sort];
  }, [sort]);
  React[isServer ? 'useEffect' : 'useLayoutEffect'](function () {
    if (!sortFn) {
      setSort(Object.keys(sortFns)[0]);
    }
  }, [setSort, sortFn]);

  var _useSafeState3 = useSafeState(Object.values(queryCache.findAll())),
      unsortedQueries = _useSafeState3[0],
      setUnsortedQueries = _useSafeState3[1];

  var _useLocalStorage6 = useLocalStorage('reactQueryDevtoolsActiveQueryHash', ''),
      activeQueryHash = _useLocalStorage6[0],
      setActiveQueryHash = _useLocalStorage6[1];

  var queries = React.useMemo(function () {
    var sorted = [].concat(unsortedQueries).sort(sortFn);

    if (sortDesc) {
      sorted.reverse();
    }

    if (!filter) {
      return sorted;
    }

    return matchSorter(sorted, filter, {
      keys: ['queryHash']
    }).filter(function (d) {
      return d.queryHash;
    });
  }, [sortDesc, sortFn, unsortedQueries, filter]);
  var activeQuery = React.useMemo(function () {
    return queries.find(function (query) {
      return query.queryHash === activeQueryHash;
    });
  }, [activeQueryHash, queries]);
  var hasFresh = queries.filter(function (q) {
    return getQueryStatusLabel(q) === 'fresh';
  }).length;
  var hasFetching = queries.filter(function (q) {
    return getQueryStatusLabel(q) === 'fetching';
  }).length;
  var hasStale = queries.filter(function (q) {
    return getQueryStatusLabel(q) === 'stale';
  }).length;
  var hasInactive = queries.filter(function (q) {
    return getQueryStatusLabel(q) === 'inactive';
  }).length;
  React.useEffect(function () {
    if (isOpen) {
      var unsubscribe = queryCache.subscribe(function () {
        setUnsortedQueries(Object.values(queryCache.getAll()));
      }); // re-subscribing after the panel is closed and re-opened won't trigger the callback,
      // So we'll manually populate our state

      setUnsortedQueries(Object.values(queryCache.getAll()));
      return unsubscribe;
    }

    return undefined;
  }, [isOpen, sort, sortFn, sortDesc, setUnsortedQueries, queryCache]);

  var handleRefetch = function handleRefetch() {
    var promise = activeQuery == null ? void 0 : activeQuery.fetch();
    promise == null ? void 0 : promise.catch(noop);
  };

  return /*#__PURE__*/React.createElement(ThemeProvider, {
    theme: theme
  }, /*#__PURE__*/React.createElement(Panel, _extends({
    ref: ref,
    className: "ReactQueryDevtoolsPanel",
    "aria-label": "React Query Devtools Panel",
    id: "ReactQueryDevtoolsPanel"
  }, panelProps), /*#__PURE__*/React.createElement("style", {
    dangerouslySetInnerHTML: {
      __html: "\n            .ReactQueryDevtoolsPanel * {\n              scrollbar-color: " + theme.backgroundAlt + " " + theme.gray + ";\n            }\n\n            .ReactQueryDevtoolsPanel *::-webkit-scrollbar, .ReactQueryDevtoolsPanel scrollbar {\n              width: 1em;\n              height: 1em;\n            }\n\n            .ReactQueryDevtoolsPanel *::-webkit-scrollbar-track, .ReactQueryDevtoolsPanel scrollbar-track {\n              background: " + theme.backgroundAlt + ";\n            }\n\n            .ReactQueryDevtoolsPanel *::-webkit-scrollbar-thumb, .ReactQueryDevtoolsPanel scrollbar-thumb {\n              background: " + theme.gray + ";\n              border-radius: .5em;\n              border: 3px solid " + theme.backgroundAlt + ";\n            }\n          "
    }
  }), /*#__PURE__*/React.createElement("div", {
    style: {
      position: 'absolute',
      left: 0,
      top: 0,
      width: '100%',
      height: '4px',
      marginBottom: '-4px',
      cursor: 'row-resize',
      zIndex: 100000
    },
    onMouseDown: handleDragStart
  }), /*#__PURE__*/React.createElement("div", {
    style: {
      flex: '1 1 500px',
      minHeight: '40%',
      maxHeight: '100%',
      overflow: 'auto',
      borderRight: "1px solid " + theme.grayAlt,
      display: isOpen ? 'flex' : 'none',
      flexDirection: 'column'
    }
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      padding: '.5em',
      background: theme.backgroundAlt,
      display: 'flex',
      justifyContent: 'space-between',
      alignItems: 'center'
    }
  }, /*#__PURE__*/React.createElement(Logo, {
    "aria-hidden": true,
    style: {
      marginRight: '.5em'
    }
  }), /*#__PURE__*/React.createElement("div", {
    style: {
      display: 'flex',
      flexDirection: 'column'
    }
  }, /*#__PURE__*/React.createElement(QueryKeys, {
    style: {
      marginBottom: '.5em'
    }
  }, /*#__PURE__*/React.createElement(QueryKey, {
    style: {
      background: theme.success,
      opacity: hasFresh ? 1 : 0.3
    }
  }, "fresh ", /*#__PURE__*/React.createElement(Code, null, "(", hasFresh, ")")), ' ', /*#__PURE__*/React.createElement(QueryKey, {
    style: {
      background: theme.active,
      opacity: hasFetching ? 1 : 0.3
    }
  }, "fetching ", /*#__PURE__*/React.createElement(Code, null, "(", hasFetching, ")")), ' ', /*#__PURE__*/React.createElement(QueryKey, {
    style: {
      background: theme.warning,
      color: 'black',
      textShadow: '0',
      opacity: hasStale ? 1 : 0.3
    }
  }, "stale ", /*#__PURE__*/React.createElement(Code, null, "(", hasStale, ")")), ' ', /*#__PURE__*/React.createElement(QueryKey, {
    style: {
      background: theme.gray,
      opacity: hasInactive ? 1 : 0.3
    }
  }, "inactive ", /*#__PURE__*/React.createElement(Code, null, "(", hasInactive, ")"))), /*#__PURE__*/React.createElement("div", {
    style: {
      display: 'flex',
      alignItems: 'center'
    }
  }, /*#__PURE__*/React.createElement(Input, {
    placeholder: "Filter",
    "aria-label": "Filter by queryhash",
    value: filter != null ? filter : '',
    onChange: function onChange(e) {
      return setFilter(e.target.value);
    },
    onKeyDown: function onKeyDown(e) {
      if (e.key === 'Escape') setFilter('');
    },
    style: {
      flex: '1',
      marginRight: '.5em'
    }
  }), !filter ? /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement(Select, {
    "aria-label": "Sort queries",
    value: sort,
    onChange: function onChange(e) {
      return setSort(e.target.value);
    },
    style: {
      flex: '1',
      minWidth: 75,
      marginRight: '.5em'
    }
  }, Object.keys(sortFns).map(function (key) {
    return /*#__PURE__*/React.createElement("option", {
      key: key,
      value: key
    }, "Sort by ", key);
  })), /*#__PURE__*/React.createElement(Button, {
    type: "button",
    onClick: function onClick() {
      return setSortDesc(function (old) {
        return !old;
      });
    },
    style: {
      padding: '.3em .4em'
    }
  }, sortDesc ? '⬇ Desc' : '⬆ Asc')) : null))), /*#__PURE__*/React.createElement("div", {
    style: {
      overflowY: 'auto',
      flex: '1'
    }
  }, queries.map(function (query, i) {
    var isDisabled = query.getObserversCount() > 0 && !query.isActive();
    return /*#__PURE__*/React.createElement("div", {
      key: query.queryHash || i,
      role: "button",
      "aria-label": "Open query details for " + query.queryHash,
      onClick: function onClick() {
        return setActiveQueryHash(activeQueryHash === query.queryHash ? '' : query.queryHash);
      },
      style: {
        display: 'flex',
        borderBottom: "solid 1px " + theme.grayAlt,
        cursor: 'pointer',
        background: query === activeQuery ? 'rgba(255,255,255,.1)' : undefined
      }
    }, /*#__PURE__*/React.createElement("div", {
      style: {
        flex: '0 0 auto',
        width: '2em',
        height: '2em',
        background: getQueryStatusColor(query, theme),
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontWeight: 'bold',
        textShadow: getQueryStatusLabel(query) === 'stale' ? '0' : '0 0 10px black',
        color: getQueryStatusLabel(query) === 'stale' ? 'black' : 'white'
      }
    }, query.getObserversCount()), isDisabled ? /*#__PURE__*/React.createElement("div", {
      style: {
        flex: '0 0 auto',
        height: '2em',
        background: theme.gray,
        display: 'flex',
        alignItems: 'center',
        fontWeight: 'bold',
        padding: '0 0.5em'
      }
    }, "disabled") : null, /*#__PURE__*/React.createElement(Code, {
      style: {
        padding: '.5em'
      }
    }, "" + query.queryHash));
  }))), activeQuery ? /*#__PURE__*/React.createElement(ActiveQueryPanel, null, /*#__PURE__*/React.createElement("div", {
    style: {
      padding: '.5em',
      background: theme.backgroundAlt,
      position: 'sticky',
      top: 0,
      zIndex: 1
    }
  }, "Query Details"), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: '.5em'
    }
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      marginBottom: '.5em',
      display: 'flex',
      alignItems: 'stretch',
      justifyContent: 'space-between'
    }
  }, /*#__PURE__*/React.createElement(Code, {
    style: {
      lineHeight: '1.8em'
    }
  }, /*#__PURE__*/React.createElement("pre", {
    style: {
      margin: 0,
      padding: 0,
      overflow: 'auto'
    }
  }, JSON.stringify(activeQuery.queryKey, null, 2))), /*#__PURE__*/React.createElement("span", {
    style: {
      padding: '0.3em .6em',
      borderRadius: '0.4em',
      fontWeight: 'bold',
      textShadow: '0 2px 10px black',
      background: getQueryStatusColor(activeQuery, theme),
      flexShrink: 0
    }
  }, getQueryStatusLabel(activeQuery))), /*#__PURE__*/React.createElement("div", {
    style: {
      marginBottom: '.5em',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between'
    }
  }, "Observers: ", /*#__PURE__*/React.createElement(Code, null, activeQuery.getObserversCount())), /*#__PURE__*/React.createElement("div", {
    style: {
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between'
    }
  }, "Last Updated:", ' ', /*#__PURE__*/React.createElement(Code, null, new Date(activeQuery.state.dataUpdatedAt).toLocaleTimeString()))), /*#__PURE__*/React.createElement("div", {
    style: {
      background: theme.backgroundAlt,
      padding: '.5em',
      position: 'sticky',
      top: 0,
      zIndex: 1
    }
  }, "Actions"), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: '0.5em'
    }
  }, /*#__PURE__*/React.createElement(Button, {
    type: "button",
    onClick: handleRefetch,
    disabled: activeQuery.state.isFetching,
    style: {
      background: theme.active
    }
  }, "Refetch"), ' ', /*#__PURE__*/React.createElement(Button, {
    type: "button",
    onClick: function onClick() {
      return queryClient.invalidateQueries(activeQuery);
    },
    style: {
      background: theme.warning,
      color: theme.inputTextColor
    }
  }, "Invalidate"), ' ', /*#__PURE__*/React.createElement(Button, {
    type: "button",
    onClick: function onClick() {
      return queryClient.resetQueries(activeQuery);
    },
    style: {
      background: theme.gray
    }
  }, "Reset"), ' ', /*#__PURE__*/React.createElement(Button, {
    type: "button",
    onClick: function onClick() {
      return queryClient.removeQueries(activeQuery);
    },
    style: {
      background: theme.danger
    }
  }, "Remove")), /*#__PURE__*/React.createElement("div", {
    style: {
      background: theme.backgroundAlt,
      padding: '.5em',
      position: 'sticky',
      top: 0,
      zIndex: 1
    }
  }, "Data Explorer"), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: '.5em'
    }
  }, /*#__PURE__*/React.createElement(Explorer, {
    label: "Data",
    value: activeQuery == null ? void 0 : (_activeQuery$state = activeQuery.state) == null ? void 0 : _activeQuery$state.data,
    defaultExpanded: {}
  })), /*#__PURE__*/React.createElement("div", {
    style: {
      background: theme.backgroundAlt,
      padding: '.5em',
      position: 'sticky',
      top: 0,
      zIndex: 1
    }
  }, "Query Explorer"), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: '.5em'
    }
  }, /*#__PURE__*/React.createElement(Explorer, {
    label: "Query",
    value: activeQuery,
    defaultExpanded: {
      queryKey: true
    }
  }))) : null));
});