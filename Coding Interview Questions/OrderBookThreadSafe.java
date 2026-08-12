import java.util.*;
import java.util.concurrent.locks.*;

enum OrderType { BUY, SELL }
enum ExecType { LIMIT, MARKET }

class Order {
    final String id;
    final OrderType type;
    final ExecType execType;
    final int price;     // ignored for MARKET
    int remaining;
    long timestamp;

    public Order(String id, OrderType type, ExecType execType, int price, int qty) {
        this.id = id;
        this.type = type;
        this.execType = execType;
        this.price = price;
        this.remaining = qty;
    }
}

class OrderBook {

    // Concurrency
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock r = rwLock.readLock();
    private final Lock w = rwLock.writeLock();

    // Price levels
    private final PriorityQueue<Integer> buyPrices =
            new PriorityQueue<>(Comparator.reverseOrder()); // max-heap
    private final PriorityQueue<Integer> sellPrices =
            new PriorityQueue<>(); // min-heap

    // price -> FIFO queue
    private final Map<Integer, Deque<Order>> buyMap = new HashMap<>();
    private final Map<Integer, Deque<Order>> sellMap = new HashMap<>();

    // order lookup
    private final Map<String, Order> orderMap = new HashMap<>();

    private long seq = 0;

    // =========================
    // Public APIs
    // =========================
    public void addOrder(Order order) {
        w.lock();
        try {
            order.timestamp = seq++;
            orderMap.put(order.id, order);

            if (order.type == OrderType.BUY) {
                matchBuy(order);
            } else {
                matchSell(order);
            }

            // Only LIMIT orders rest in book
            if (order.remaining > 0 && order.execType == ExecType.LIMIT) {
                addToBook(order);
            } else {
                orderMap.remove(order.id);
            }

        } finally {
            w.unlock();
        }
    }

    public void cancelOrder(String id) {
        w.lock();
        try {
            Order o = orderMap.remove(id);
            if (o != null) {
                o.remaining = 0; // lazy removal
            }
        } finally {
            w.unlock();
        }
    }

    public Integer getBestBid() {
        r.lock();
        try {
            return cleanTop(buyPrices, buyMap);
        } finally {
            r.unlock();
        }
    }

    public Integer getBestAsk() {
        r.lock();
        try {
            return cleanTop(sellPrices, sellMap);
        } finally {
            r.unlock();
        }
    }

    // =========================
    // Matching Logic
    // =========================
    private void matchBuy(Order buy) {
        while (buy.remaining > 0 && !sellPrices.isEmpty()) {
            Integer bestSell = cleanTop(sellPrices, sellMap);
            if (bestSell == null) break;

            // LIMIT constraint
            if (buy.execType == ExecType.LIMIT && bestSell > buy.price) break;

            Deque<Order> queue = sellMap.get(bestSell);

            while (!queue.isEmpty() && buy.remaining > 0) {
                Order sell = queue.peek();

                int traded = Math.min(buy.remaining, sell.remaining);
                buy.remaining -= traded;
                sell.remaining -= traded;

                System.out.println("TRADE BUY " + buy.id +
                        " with SELL " + sell.id +
                        " qty=" + traded + " @ " + bestSell);

                if (sell.remaining == 0) {
                    queue.poll();
                    orderMap.remove(sell.id);
                }
            }

            if (queue.isEmpty()) {
                sellMap.remove(bestSell);
                sellPrices.poll();
            }
        }
    }

    private void matchSell(Order sell) {
        while (sell.remaining > 0 && !buyPrices.isEmpty()) {
            Integer bestBuy = cleanTop(buyPrices, buyMap);
            if (bestBuy == null) break;

            if (sell.execType == ExecType.LIMIT && bestBuy < sell.price) break;

            Deque<Order> queue = buyMap.get(bestBuy);

            while (!queue.isEmpty() && sell.remaining > 0) {
                Order buy = queue.peek();

                int traded = Math.min(sell.remaining, buy.remaining);
                sell.remaining -= traded;
                buy.remaining -= traded;

                System.out.println("TRADE SELL " + sell.id +
                        " with BUY " + buy.id +
                        " qty=" + traded + " @ " + bestBuy);

                if (buy.remaining == 0) {
                    queue.poll();
                    orderMap.remove(buy.id);
                }
            }

            if (queue.isEmpty()) {
                buyMap.remove(bestBuy);
                buyPrices.poll();
            }
        }
    }

    // =========================
    // Helpers
    // =========================
    private void addToBook(Order o) {
        Map<Integer, Deque<Order>> map =
                o.type == OrderType.BUY ? buyMap : sellMap;

        PriorityQueue<Integer> heap =
                o.type == OrderType.BUY ? buyPrices : sellPrices;

        map.putIfAbsent(o.price, new ArrayDeque<>());

        if (map.get(o.price).isEmpty()) {
            heap.offer(o.price);
        }

        map.get(o.price).offer(o);
    }

    private Integer cleanTop(PriorityQueue<Integer> heap,
                             Map<Integer, Deque<Order>> map) {
        while (!heap.isEmpty()) {
            int p = heap.peek();
            Deque<Order> q = map.get(p);
            if (q == null || q.isEmpty()) {
                heap.poll();
                map.remove(p);
            } else {
                return p;
            }
        }
        return null;
    }
public static void main(String[] args) {
OrderBook ob = new OrderBook();

ob.addOrder(new Order("B1", OrderType.BUY, ExecType.LIMIT, 100, 10));
ob.addOrder(new Order("S1", OrderType.SELL, ExecType.LIMIT, 101, 5));

// Market order (ignores price)
ob.addOrder(new Order("B2", OrderType.BUY, ExecType.MARKET, 0, 7));

System.out.println("Best Bid: " + ob.getBestBid());
System.out.println("Best Ask: " + ob.getBestAsk());
}
}
