/**
 * Copyright (c) 2013-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license at
 * https://github.com/facebook/fbjs/blob/master/LICENSE
 */
/**
 * Performs equality by iterating through keys on an object and returning false
 * when any key has values which are not strictly equal between the arguments.
 * Returns true when the values of all keys are strictly equal.

 * @pbeshai modification of shallowEqual to take into consideration a map providing
 * equals functions
 */
export default function shallowEqual(objA: any, objB: any, equalMap?: any): boolean;
