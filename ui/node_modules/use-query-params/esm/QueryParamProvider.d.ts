import * as React from 'react';
import { HistoryLocation, PushReplaceHistory } from './types';
import { ExtendedStringifyOptions } from 'serialize-query-params';
/**
 * Subset of a @reach/router history object. We only
 * care about the navigate function.
 */
interface ReachHistory {
    navigate: (to: string, options?: {
        state?: any;
        replace?: boolean;
    }) => void;
    location: Location;
}
/**
 * Helper to produce the context value falling back to
 * window history and location if not provided.
 */
export declare function getLocationProps({ history, location, }?: Partial<HistoryLocation>): HistoryLocation;
/**
 * Props for the Provider component, used to hook the active routing
 * system into our controls.
 */
interface QueryParamProviderProps {
    /** Main app goes here */
    children: React.ReactNode;
    /** `Route` from react-router */
    ReactRouterRoute?: React.ComponentClass | React.FunctionComponent;
    /** `globalHistory` from @reach/router */
    reachHistory?: ReachHistory;
    /** Manually provided history that meets the { replace, push } interface */
    history?: PushReplaceHistory;
    /**
     * Override location object, otherwise window.location or the
     * location provided by the active routing system is used.
     */
    location?: Location;
    /**
     * Options to customize the stringifying of the query string
     * These are passed directly to query-string.stringify, with
     * the exception of transformSearchString which runs on the result
     * of stringify if provided.
     */
    stringifyOptions?: ExtendedStringifyOptions;
}
/**
 * Context provider for query params to have access to the
 * active routing system, enabling updates to the URL.
 */
export declare function QueryParamProvider({ children, ReactRouterRoute, reachHistory, history, location, stringifyOptions, }: QueryParamProviderProps): JSX.Element;
export default QueryParamProvider;
