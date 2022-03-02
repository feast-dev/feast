import * as React from 'react';
export declare function useUpdateRefIfShallowNew<T>(ref: React.MutableRefObject<T>, newValue: T, isEqual?: (objA: NonNullable<T>, objB: NonNullable<T>) => boolean): void;
export declare function getSSRSafeSearchString(location: Location | undefined): string;
