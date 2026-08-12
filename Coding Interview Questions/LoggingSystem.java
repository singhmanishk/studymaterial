import java.util.*;

enum Severity {
    INFO, WARN, ERROR
}

class Log {
    String message;
    Severity severity;
    long timestamp;

    public Log(String message, Severity severity, long timestamp) {
        this.message = message;
        this.severity = severity;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "[" + severity + "] " + message + " @ " + timestamp;
    }
}

class LoggingSystem {

    private final Deque<Log> logs;
    private final Map<Severity, List<Log>> severityMap;
    private final int capacity;

    public LoggingSystem(int capacity) {
        this.capacity = capacity;
        this.logs = new LinkedList<>();
        this.severityMap = new HashMap<>();
    }

    // =========================
    // Add Log
    // =========================
    public void log(String message, Severity severity) {
        long now = System.currentTimeMillis();

        Log log = new Log(message, severity, now);

        logs.addLast(log);

        severityMap.putIfAbsent(severity, new ArrayList<>());
        severityMap.get(severity).add(log);

        // Maintain capacity (like circular buffer)
        if (logs.size() > capacity) {
            Log old = logs.removeFirst();
            severityMap.get(old.severity).remove(old);
        }
    }

    // =========================
    // Get Last N Logs
    // =========================
    public List<Log> getLastNLogs(int n) {
        List<Log> result = new ArrayList<>();

        Iterator<Log> it = logs.descendingIterator();

        while (it.hasNext() && n-- > 0) {
            result.add(it.next());
        }

        return result;
    }

    // =========================
    // Get by Severity
    // =========================
    public List<Log> getLogsBySeverity(Severity severity) {
        return severityMap.getOrDefault(severity, new ArrayList<>());
    }

    // =========================
    // Get by Time Range
    // =========================
    public List<Log> getLogsByTimeRange(long start, long end) {
        List<Log> result = new ArrayList<>();

        for (Log log : logs) {
            if (log.timestamp >= start && log.timestamp <= end) {
                result.add(log);
            }
        }

        return result;
    }
	
	public static void main(String[] args) {
        LoggingSystem logger = new LoggingSystem(5);

        logger.log("System started", Severity.INFO);
        logger.log("Connection issue", Severity.WARN);
        logger.log("Null pointer exception", Severity.ERROR);

        System.out.println(logger.getLastNLogs(2));
        System.out.println(logger.getLogsBySeverity(Severity.ERROR));
    }
}