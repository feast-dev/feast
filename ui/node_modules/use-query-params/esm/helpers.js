import * as React from 'react';
import { extract } from 'query-string';
import shallowEqual from './shallowEqual';
export function useUpdateRefIfShallowNew(ref, newValue, isEqual) {
    if (isEqual === void 0) { isEqual = shallowEqual; }
    var hasNew = ((ref.current == null || newValue == null) && ref.current === newValue) ||
        !isEqual(ref.current, newValue);
    React.useEffect(function () {
        if (hasNew) {
            ref.current = newValue;
        }
    }, [ref, newValue, hasNew]);
}
export function getSSRSafeSearchString(location) {
    // handle checking SSR (#13)
    if (typeof location === 'object') {
        // in browser
        if (typeof window !== 'undefined') {
            return location.search;
        }
        else {
            return extract("" + location.pathname + (location.search ? location.search : ''));
        }
    }
    return '';
}
