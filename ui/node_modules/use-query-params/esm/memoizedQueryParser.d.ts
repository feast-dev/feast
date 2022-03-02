import { EncodedQuery } from 'serialize-query-params';
export declare const makeMemoizedQueryParser: (initialSearchString?: string | undefined) => (newSearchString: string) => EncodedQuery;
export declare const sharedMemoizedQueryParser: (newSearchString: string) => EncodedQuery;
