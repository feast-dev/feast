import { QueryParamConfig } from './types';
/**
 * Wrap a given parameter with a default value when undefined or null (optionally, default includes null)
 * @param param QueryParamConfig - { encode, decode} to serialize a parameter
 * @param defaultValue A default value
 * @param includeNull
 */
export declare function withDefault<D, DefaultType extends D2, D2 = D>(param: QueryParamConfig<D, D2>, defaultValue: DefaultType, includeNull?: false | undefined): QueryParamConfig<D, Exclude<D2, undefined> | DefaultType>;
export declare function withDefault<D, DefaultType extends D2, D2 = D>(param: QueryParamConfig<D, D2>, defaultValue: DefaultType, includeNull?: true): QueryParamConfig<D, Exclude<D2, null | undefined> | DefaultType>;
export default withDefault;
