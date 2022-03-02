import { updateLocation, updateInLocation, } from 'serialize-query-params';
/**
 * Creates a new location object containing the specified query changes.
 * If replaceIn or pushIn are used as the updateType, then parameters
 * not specified in queryReplacements are retained. If replace or push
 * are used, only the values in queryReplacements will be available.
 * The default is pushIn.
 */
export function createLocationWithChanges(queryReplacements, location, updateType, stringifyOptions) {
    if (updateType === void 0) { updateType = 'pushIn'; }
    switch (updateType) {
        case 'replace':
        case 'push':
            return updateLocation(queryReplacements, location, stringifyOptions);
        case 'replaceIn':
        case 'pushIn':
        default:
            return updateInLocation(queryReplacements, location, stringifyOptions);
    }
}
/**
 * Updates the URL to the new location.
 */
export function updateUrlQuery(history, location, updateType) {
    if (updateType === void 0) { updateType = 'pushIn'; }
    switch (updateType) {
        case 'pushIn':
        case 'push':
            history.push(location);
            break;
        case 'replaceIn':
        case 'replace':
        default:
            history.replace(location);
            break;
    }
}
