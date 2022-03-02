import * as React from 'react';
import { StringParam } from 'serialize-query-params';
import { getSSRSafeSearchString, useUpdateRefIfShallowNew } from './helpers';
import { useLocationContext } from './LocationProvider';
import { sharedMemoizedQueryParser } from './memoizedQueryParser';
import shallowEqual from './shallowEqual';
/**
 * Helper to get the latest decoded value with smart caching.
 * Abstracted into its own function to allow re-use in a functional setter (#26)
 */
function getLatestDecodedValue(location, name, paramConfig, paramConfigRef, encodedValueCacheRef, decodedValueCacheRef) {
    var _a;
    // check if we have a new param config
    var hasNewParamConfig = !shallowEqual(paramConfigRef.current, paramConfig);
    var isValueEqual = (_a = paramConfig.equals) !== null && _a !== void 0 ? _a : shallowEqual;
    // read in the parsed query
    var parsedQuery = sharedMemoizedQueryParser(getSSRSafeSearchString(location) // get the latest location object
    );
    // read in the encoded string value (we have to check cache if available because
    // sometimes the query string changes so we get a new parsedQuery but this value
    // didn't change, so we should avoid generating a new array or whatever value)
    var hasNewEncodedValue = !shallowEqual(encodedValueCacheRef.current, parsedQuery[name]);
    var encodedValue = hasNewEncodedValue
        ? parsedQuery[name]
        : encodedValueCacheRef.current;
    // only decode if we have changes to encoded value or the config.
    // check for undefined to handle initial case
    if (!hasNewEncodedValue &&
        !hasNewParamConfig &&
        decodedValueCacheRef.current !== undefined) {
        return decodedValueCacheRef.current;
    }
    var newDecodedValue = paramConfig.decode(encodedValue);
    var hasNewDecodedValue = ((decodedValueCacheRef.current == null || newDecodedValue == null) &&
        decodedValueCacheRef.current === newDecodedValue) ||
        !isValueEqual(decodedValueCacheRef.current, newDecodedValue);
    // if we have a new decoded value use it, otherwise use cached
    return hasNewDecodedValue
        ? newDecodedValue
        : decodedValueCacheRef.current;
}
/**
 * Given a query param name and query parameter configuration ({ encode, decode })
 * return the decoded value and a setter for updating it.
 *
 * The setter takes two arguments (newValue, updateType) where updateType
 * is one of 'replace' | 'replaceIn' | 'push' | 'pushIn', defaulting to
 * 'pushIn'.
 *
 * You may optionally pass in a rawQuery object, otherwise the query is derived
 * from the location available in the context.
 *
 * D = decoded type
 * D2 = return value from decode (typically same as D)
 */
export var useQueryParam = function (name, paramConfig) {
    if (paramConfig === void 0) { paramConfig = StringParam; }
    var _a = useLocationContext(), location = _a.location, getLocation = _a.getLocation, setLocation = _a.setLocation;
    // read in the raw query
    var parsedQuery = sharedMemoizedQueryParser(getSSRSafeSearchString(location));
    // make caches
    var encodedValueCacheRef = React.useRef();
    var paramConfigRef = React.useRef(paramConfig);
    var decodedValueCacheRef = React.useRef();
    var decodedValue = getLatestDecodedValue(location, name, paramConfig, paramConfigRef, encodedValueCacheRef, decodedValueCacheRef);
    // update cached values in a useEffect
    useUpdateRefIfShallowNew(encodedValueCacheRef, parsedQuery[name]);
    useUpdateRefIfShallowNew(paramConfigRef, paramConfig);
    useUpdateRefIfShallowNew(decodedValueCacheRef, decodedValue, paramConfig.equals);
    // create the setter, memoizing via useCallback
    var setValueDeps = {
        paramConfig: paramConfig,
        name: name,
        setLocation: setLocation,
        getLocation: getLocation,
    };
    var setValueDepsRef = React.useRef(setValueDeps);
    setValueDepsRef.current = setValueDeps;
    var setValue = React.useCallback(function setValueCallback(newValue, updateType) {
        var _a;
        var deps = setValueDepsRef.current;
        var newEncodedValue;
        // allow functional updates #26
        if (typeof newValue === 'function') {
            // get latest decoded value to pass as a fresh arg to the setter fn
            var latestValue = getLatestDecodedValue(deps.getLocation(), deps.name, deps.paramConfig, paramConfigRef, encodedValueCacheRef, decodedValueCacheRef);
            decodedValueCacheRef.current = latestValue; // keep cache in sync
            newEncodedValue = deps.paramConfig.encode(newValue(latestValue));
        }
        else {
            newEncodedValue = deps.paramConfig.encode(newValue);
        }
        // update the URL
        deps.setLocation((_a = {}, _a[deps.name] = newEncodedValue, _a), updateType);
    }, []);
    return [decodedValue, setValue];
};
