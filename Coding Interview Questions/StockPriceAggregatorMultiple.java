import java.util.*;
import java.util.concurrent.locks.*;

class StockPriceAggregator {

    private Map<String, StockPrice> stockMap = new HashMap<>();

    public void update(String symbol, int timestamp, int price) {
        stockMap.putIfAbsent(symbol, new StockPrice());
        stockMap.get(symbol).update(timestamp, price);
    }

    public int current(String symbol) {
        return stockMap.get(symbol).current();
    }

    public int maximum(String symbol) {
        return stockMap.get(symbol).maximum();
    }

    public int minimum(String symbol) {
        return stockMap.get(symbol).minimum();
    }

}

class StockPrice {

    private final Map<Integer, Integer> timePriceMap;
    private final PriorityQueue<int[]> maxHeap;
    private final PriorityQueue<int[]> minHeap;

    private int latestTime;

    private final ReadWriteLock rwLock;
    private final Lock readLock;
    private final Lock writeLock;

    public StockPrice() {
        timePriceMap = new HashMap<>();

        maxHeap = new PriorityQueue<>((a, b) -> b[1] - a[1]);
        minHeap = new PriorityQueue<>((a, b) -> a[1] - b[1]);

        latestTime = 0;

        rwLock = new ReentrantReadWriteLock();
        readLock = rwLock.readLock();
        writeLock = rwLock.writeLock();
    }

    // =========================
    // Update (WRITE)
    // =========================
    public void update(int timestamp, int price) {
        writeLock.lock();
        try {
            timePriceMap.put(timestamp, price);
            latestTime = Math.max(latestTime, timestamp);

            maxHeap.offer(new int[]{timestamp, price});
            minHeap.offer(new int[]{timestamp, price});

        } finally {
            writeLock.unlock();
        }
    }

    // =========================
    // Current (READ)
    // =========================
    public int current() {
        readLock.lock();
        try {
            return timePriceMap.get(latestTime);
        } finally {
            readLock.unlock();
        }
    }

    // =========================
    // Maximum (WRITE due to cleanup)
    // =========================
    public int maximum() {
        writeLock.lock();  // needed because we mutate heap
        try {
            while (true) {
                int[] top = maxHeap.peek();
                int timestamp = top[0];
                int price = top[1];

                if (timePriceMap.get(timestamp) == price) {
                    return price;
                }

                maxHeap.poll(); // remove stale entry
            }
        } finally {
            writeLock.unlock();
        }
    }

    // =========================
    // Minimum (WRITE due to cleanup)
    // =========================
    public int minimum() {
        writeLock.lock();
        try {
            while (true) {
                int[] top = minHeap.peek();
                int timestamp = top[0];
                int price = top[1];

                if (timePriceMap.get(timestamp) == price) {
                    return price;
                }

                minHeap.poll();
            }
        } finally {
            writeLock.unlock();
        }
    }
}
	public static void main(String[] args) {
		StockPrice sp = new StockPrice();

		sp.update(1, 100);
		sp.update(2, 80);
		sp.update(3, 120);

		System.out.println(sp.current()); // 120
		System.out.println(sp.maximum()); // 120
		System.out.println(sp.minimum()); // 80

		// Correction
		sp.update(2, 130);

		System.out.println(sp.maximum()); // 130
		System.out.println(sp.minimum()); // 100
	}
}
