import { StringifyOptions } from 'query-string';
import { EncodedQuery } from './types';
/**
 * options passed to query-string stringify plus an
 * addition of transformSearchString: a function that takes
 * the result of stringify and runs a transformation on it.
 * (e.g. replacing all instances of character x with y)
 */
export declare type ExtendedStringifyOptions = StringifyOptions & {
    transformSearchString?: (searchString: string) => string;
};
export declare function transformSearchStringJsonSafe(searchString: string): string;
/**
 * Update a location, wiping out parameters not included in encodedQuery
 * If a param is set to undefined it will be removed from the URL.
 */
export declare function updateLocation(encodedQuery: EncodedQuery, location: Location, stringifyOptions?: ExtendedStringifyOptions): Location;
/**
 * Update a location while retaining existing parameters.
 * If a param is set to undefined it will be removed from the URL.
 */
export declare function updateInLocation(encodedQueryReplacements: EncodedQuery, location: Location, stringifyOptions?: ExtendedStringifyOptions): Location;
