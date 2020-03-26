package tests;

import dataloader.DataLoader;
import tink.unit.Assert.*;

using tink.CoreApi;

@:asserts
class TestDataloader {
  public function new() {}

  public function testSimple() {
    final identityLoader = new DataLoader((keys: Array<Int>) -> {
      trace(keys);
      return keys.map(Success);
    });

    final promise1 = identityLoader.load(1);
    final promise2 = identityLoader.load(2);

    return Promise.inParallel([promise1, promise2])
      .next(res -> assert(res[0] == 1 && res[1] == 2));
  }
}
