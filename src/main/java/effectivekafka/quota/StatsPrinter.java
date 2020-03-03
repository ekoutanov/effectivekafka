package effectivekafka.quota;

final class StatsPrinter {
  private static final long PRINT_INTERVAL_MS = 1_000;

  private long timestampOfLastPrint = System.currentTimeMillis();
  private long lastNumberOfRecords = 0;
  private long totalNumberOfRecords = 0;

  void accumulateRecord() {
    totalNumberOfRecords++;
  }

  void maybePrintStats() {
    final var now = System.currentTimeMillis();
    final var timeElapsed = now - timestampOfLastPrint;
    if (timeElapsed > PRINT_INTERVAL_MS) {
      final var produced = totalNumberOfRecords - lastNumberOfRecords;
      final var rate = produced / (double) timeElapsed * 1000d;
      System.out.printf("Rate: %,.0f/sec%n", rate);
      lastNumberOfRecords = totalNumberOfRecords;
      timestampOfLastPrint = now;
    }
  }
}