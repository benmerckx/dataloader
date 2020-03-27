package tests;

import tink.unit.AssertionBuffer;
import helder.DataLoader;
import tink.unit.Assert.*;

using tink.CoreApi;

@:asserts
class TestDataloader {
  public function new() {}

  @:describe('builds a really really simple data loader')
  public function testSimple() {
    final identityLoader = new DataLoader((keys: Array<Int>) -> keys.map(Success));

    return identityLoader.load(1).next(v -> assert(v == 1));
  }

  @:describe('supports loading multiple keys in one call')
  public function testMultipleKeys() {
    final identityLoader = new DataLoader((keys: Array<Int>) -> keys.map(Success));
    return identityLoader.loadMany([1, 2])
      .next(values -> compareValues(asserts, [Success(1), Success(2)], values).done());
  }

  @:describe('supports loading multiple keys in one call with errors')
  public function testMultipleKeysWithError() {
    final identityLoader = new DataLoader((keys: Array<String>) ->
      keys.map(key -> if (key == 'bad') Failure(new Error('Bad Key')) else Success(key)));

    final promiseAll = identityLoader.loadMany(['a', 'b', 'bad']);

    return promiseAll.next(values ->
      compareValues(asserts, [Success('a'), Success('b'), Failure(new Error('Bad Key'))], values)
        .done());
  }

  function compareValues<V>(asserts: AssertionBuffer,
      a: Array<Outcome<V, Error>>, b: Array<Outcome<V, Error>>) {
    final desc = '(${a} == ${b})';
    if (a.length != b.length) {
      asserts.assert(false, 'Values have different length ' + desc);
      return asserts;
    }
    for (i in 0...a.length) {
      switch [a[i], b[i]] {
        case [Success(a), Success(b)] if (a != b):
          asserts.assert(false, 'Values are not equal ' + desc);
          return asserts;
        case [Success(_), Failure(_)] | [Failure(_), Success(_)]:
          asserts.assert(false, 'Values are not equal ' + desc);
          return asserts;
        default:
      }
    }
    asserts.assert(true, desc);
    return asserts;
  }
}
