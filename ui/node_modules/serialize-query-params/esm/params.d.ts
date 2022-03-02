import { QueryParamConfig } from './types';
/**
 * String values
 */
export declare const StringParam: QueryParamConfig<string | null | undefined, string | null | undefined>;
/**
 * String enum
 */
export declare const createEnumParam: <T extends string>(enumValues: T[]) => QueryParamConfig<T | null | undefined, T | null | undefined>;
/**
 * Numbers (integers or floats)
 */
export declare const NumberParam: QueryParamConfig<number | null | undefined, number | null | undefined>;
/**
 * For flat objects where values are strings
 */
export declare const ObjectParam: QueryParamConfig<{
    [key: string]: string | undefined;
} | null | undefined, {
    [key: string]: string | undefined;
} | null | undefined>;
/**
 * For flat arrays of strings, filters out undefined values during decode
 */
export declare const ArrayParam: QueryParamConfig<(string | null)[] | null | undefined, (string | null)[] | null | undefined>;
/**
 * For flat arrays of strings, filters out undefined values during decode
 */
export declare const NumericArrayParam: QueryParamConfig<(number | null)[] | null | undefined, (number | null)[] | null | undefined>;
/**
 * For any type of data, encoded via JSON.stringify
 */
export declare const JsonParam: QueryParamConfig<any, any>;
/**
 * For simple dates (YYYY-MM-DD)
 */
export declare const DateParam: QueryParamConfig<Date | null | undefined, Date | null | undefined>;
/**
 * For dates in simplified extended ISO format (YYYY-MM-DDTHH:mm:ss.sssZ or ±YYYYYY-MM-DDTHH:mm:ss.sssZ)
 */
export declare const DateTimeParam: QueryParamConfig<Date | null | undefined, Date | null | undefined>;
/**
 * For boolean values: 1 = true, 0 = false
 */
export declare const BooleanParam: QueryParamConfig<boolean | null | undefined, boolean | null | undefined>;
/**
 * For flat objects where the values are numbers
 */
export declare const NumericObjectParam: QueryParamConfig<{
    [key: string]: number | null | undefined;
} | null | undefined, {
    [key: string]: number | null | undefined;
} | null | undefined>;
/**
 * For flat arrays of strings, filters out undefined values during decode
 */
export declare const DelimitedArrayParam: QueryParamConfig<(string | null)[] | null | undefined, (string | null)[] | null | undefined>;
/**
 * For flat arrays where the values are numbers, filters out undefined values during decode
 */
export declare const DelimitedNumericArrayParam: QueryParamConfig<(number | null)[] | null | undefined, (number | null)[] | null | undefined>;
