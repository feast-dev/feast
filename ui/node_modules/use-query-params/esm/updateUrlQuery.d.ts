import { EncodedQuery, ExtendedStringifyOptions } from 'serialize-query-params';
import { PushReplaceHistory, UrlUpdateType } from './types';
/**
 * Creates a new location object containing the specified query changes.
 * If replaceIn or pushIn are used as the updateType, then parameters
 * not specified in queryReplacements are retained. If replace or push
 * are used, only the values in queryReplacements will be available.
 * The default is pushIn.
 */
export declare function createLocationWithChanges(queryReplacements: EncodedQuery, location: Location, updateType?: UrlUpdateType, stringifyOptions?: ExtendedStringifyOptions): Location;
/**
 * Updates the URL to the new location.
 */
export declare function updateUrlQuery(history: PushReplaceHistory, location: Location, updateType?: UrlUpdateType): void;
