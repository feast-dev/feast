import { parse as parseQueryString } from 'query-string';
export var makeMemoizedQueryParser = function (initialSearchString) {
    var cachedSearchString = initialSearchString;
    var cachedParsedQuery = parseQueryString(cachedSearchString || '');
    return function (newSearchString) {
        if (cachedSearchString !== newSearchString) {
            cachedSearchString = newSearchString;
            cachedParsedQuery = parseQueryString(cachedSearchString);
        }
        return cachedParsedQuery;
    };
};
export var sharedMemoizedQueryParser = makeMemoizedQueryParser();
