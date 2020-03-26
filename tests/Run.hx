package tests;

import tink.unit.TestBatch;
import tink.testrunner.Runner;

class Run {
  static function main() {
    Runner.run(TestBatch.make([new TestDataloader()])).handle(Runner.exit);
  }
}
