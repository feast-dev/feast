import * as React from 'react';
import { QueryParamConfigMap, DecodedValueMap } from 'serialize-query-params';
import { SetQuery } from './types';
declare type Omit<T, K> = Pick<T, Exclude<keyof T, K>>;
declare type Diff<T, K> = Omit<T, keyof K>;
export interface InjectedQueryProps<QPCMap extends QueryParamConfigMap> {
    query: DecodedValueMap<QPCMap>;
    setQuery: SetQuery<QPCMap>;
}
/**
 * HOC to provide query parameters via props `query` and `setQuery`
 * NOTE: I couldn't get type to automatically infer generic when
 * using the format withQueryParams(config)(component), so I switched
 * to withQueryParams(config, component).
 * See: https://github.com/microsoft/TypeScript/issues/30134
 */
export declare function withQueryParams<QPCMap extends QueryParamConfigMap, P extends InjectedQueryProps<QPCMap>>(paramConfigMap: QPCMap, WrappedComponent: React.ComponentType<P>): React.FC<Diff<P, InjectedQueryProps<QPCMap>>>;
export default withQueryParams;
/**
 * HOC to provide query parameters via props mapToProps (similar to
 * react-redux connect style mapStateToProps)
 * NOTE: I couldn't get type to automatically infer generic when
 * using the format withQueryParams(config)(component), so I switched
 * to withQueryParams(config, component).
 * See: https://github.com/microsoft/TypeScript/issues/30134
 */
export declare function withQueryParamsMapped<QPCMap extends QueryParamConfigMap, MappedProps extends object, P extends MappedProps>(paramConfigMap: QPCMap, mapToProps: (query: DecodedValueMap<QPCMap>, setQuery: SetQuery<QPCMap>, props: Diff<P, MappedProps>) => MappedProps, WrappedComponent: React.ComponentType<P>): React.FC<Diff<P, MappedProps>>;
