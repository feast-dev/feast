import * as React from 'react';
import { EncodedQuery, ExtendedStringifyOptions } from 'serialize-query-params';
import { UrlUpdateType, HistoryLocation } from './types';
/**
 * Shape of the LocationProviderContext, which the hooks consume to read and
 * update the URL state.
 */
declare type LocationProviderContext = {
    location: Location;
    getLocation: () => Location;
    setLocation: (queryReplacements: EncodedQuery, updateType?: UrlUpdateType) => void;
};
export declare const LocationContext: React.Context<LocationProviderContext>;
export declare function useLocationContext(): LocationProviderContext;
/**
 * Props for the LocationProvider.
 */
declare type LocationProviderProps = HistoryLocation & {
    /** Main app goes here */
    children: React.ReactNode;
    stringifyOptions?: ExtendedStringifyOptions;
};
/**
 * An internal-only context provider which provides down the most
 * recent location object and a callback to update the history.
 */
export declare function LocationProvider({ history, location, children, stringifyOptions, }: LocationProviderProps): JSX.Element;
export {};
