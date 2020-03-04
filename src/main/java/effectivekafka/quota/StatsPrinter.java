package effectivekafka.quota;

final class StatsPrinter {
  private static final long PRINT_INTERVAL_MS = 1_000;

  private final long startTime = System.currentTimeMillis();
  private long timestampOfLastPrint = startTime;
  private long lastRecordCount = 0;
  private long totalRecordCount = 0;

  void accumulateRecord() {
    totalRecordCount++;
  }

  void maybePrintStats() {
    final var now = System.currentTimeMillis();
    final var timeElapsed = now - timestampOfLastPrint;
    if (timeElapsed > PRINT_INTERVAL_MS) {
      final var timeSinceStart = now - startTime;
      final var produced = totalRecordCount - lastRecordCount;
      final var rate = produced / (double) timeElapsed * 1000d;
      System.out.printf("Elapsed: %,d s; Rate: %,.0f rec/s%n", 
                        timeSinceStart / 1000, rate);
      lastRecordCount = totalRecordCount;
      timestampOfLastPrint = now;
    }
  }
}