package effectivekafka.quota;

import static java.lang.System.*;

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
    final var lastPrintAgo = now - timestampOfLastPrint;
    if (lastPrintAgo > PRINT_INTERVAL_MS) {
      final var elapsedTime = now - startTime;
      final var periodRecords = totalRecordCount - lastRecordCount;
      final var currentRate = rate(periodRecords, lastPrintAgo);
      final var averageRate = rate(lastRecordCount, elapsedTime);
      out.printf("Elapsed: %,d s; " + 
                 "Rate: current %,.0f rec/s, average %,.0f rec/s%n",
                 elapsedTime / 1000, currentRate, averageRate);
      lastRecordCount = totalRecordCount;
      timestampOfLastPrint = now;
    }
  }
  
  private double rate(long quantity, long timeMs) {
    return quantity / (double) timeMs * 1000d;
  }
}