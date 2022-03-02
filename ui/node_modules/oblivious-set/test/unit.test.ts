import AsyncTestUtil from 'async-test-util';
import * as assert from 'assert';

import {
    ObliviousSet
} from '../src/index';

describe('unit.test.js', () => {
    it('create, add, has, get, clear', () => {
        const set = new ObliviousSet(100);
        set.add('foobar');
        const has = set.has('foobar');
        assert.ok(has);

        set.clear();
        const hasAfter = set.has('foobar');
        assert.strictEqual(false, hasAfter);
    });
    it('.removeTooOldValues() should clear the old values when a new one is given', async () => {
        const set = new ObliviousSet(100);
        set.add('foobar');
        assert.ok(set.has('foobar'));

        await AsyncTestUtil.wait(200);
        set.add('foobar2');
        await AsyncTestUtil.wait(100);

        const has = set.has('foobar');

        assert.strictEqual(false, has);
    });
    it('.removeTooOldValues() should NOT clear to young values when a new one is given', async () => {
        const set = new ObliviousSet(500);
        set.add('foobar');
        assert.ok(set.has('foobar'));

        await AsyncTestUtil.wait(50);
        set.add('foobar2');
        assert.ok(set.has('foobar'));
    });
    it('should clear the value after its ttl', async () => {
        const set = new ObliviousSet(100);
        set.add('foobar');

        await AsyncTestUtil.wait(200);
        set.add('foobar2');
        await AsyncTestUtil.wait(100);

        const has = set.has('foobar');
        assert.strictEqual(false, has);
    });
});
