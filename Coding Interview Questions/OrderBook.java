import java.util.*;

enum OrderType {
    BUY, SELL
}

class Order {
    String id;
    OrderType type;
    int price;
    int quantity;
    int remaining;
    long timestamp;

    public Order(String id, OrderType type, int price, int quantity) {
        this.id = id;
        this.type = type;
        this.price = price;
        this.quantity = quantity;
        this.remaining = quantity;
    }
}

class OrderBook {

    // Price levels
    private PriorityQueue<Integer> buyPrices;   // max heap
    private PriorityQueue<Integer> sellPrices;  // min heap

    // price → queue of orders (FIFO)
    private Map<Integer, Deque<Order>> buyMap;
    private Map<Integer, Deque<Order>> sellMap;

    // orderId → Order
    private Map<String, Order> orderMap;

    private long sequence = 0; // timestamp

    public OrderBook() {
        buyPrices = new PriorityQueue<>(Comparator.reverseOrder());
        sellPrices = new PriorityQueue<>();

        buyMap = new HashMap<>();
        sellMap = new HashMap<>();
        orderMap = new HashMap<>();
    }

    // =========================
    // Add Order
    // =========================
    public void addOrder(Order order) {
        order.timestamp = sequence++;
        orderMap.put(order.id, order);

        if (order.type == OrderType.BUY) {
            matchBuy(order);
        } else {
            matchSell(order);
        }

        // If still remaining, add to book
        if (order.remaining > 0) {
            addToBook(order);
        } else {
            orderMap.remove(order.id);
        }
    }

    // =========================
    // Matching Logic
    // =========================
    private void matchBuy(Order buy) {
        while (buy.remaining > 0 && !sellPrices.isEmpty()) {
            int bestSellPrice = sellPrices.peek();

            if (bestSellPrice > buy.price) break;

            Deque<Order> queue = sellMap.get(bestSellPrice);

            while (!queue.isEmpty() && buy.remaining > 0) {
                Order sell = queue.peek();

                int traded = Math.min(buy.remaining, sell.remaining);
                buy.remaining -= traded;
                sell.remaining -= traded;

                System.out.println("TRADE: BUY " + buy.id +
                        " & SELL " + sell.id + " qty=" + traded);

                if (sell.remaining == 0) {
                    queue.poll();
                    orderMap.remove(sell.id);
                }
            }

            if (queue.isEmpty()) {
                sellMap.remove(bestSellPrice);
                sellPrices.poll();
            }
        }
    }

    private void matchSell(Order sell) {
        while (sell.remaining > 0 && !buyPrices.isEmpty()) {
            int bestBuyPrice = buyPrices.peek();

            if (bestBuyPrice < sell.price) break;

            Deque<Order> queue = buyMap.get(bestBuyPrice);

            while (!queue.isEmpty() && sell.remaining > 0) {
                Order buy = queue.peek();

                int traded = Math.min(sell.remaining, buy.remaining);
                sell.remaining -= traded;
                buy.remaining -= traded;

                System.out.println("TRADE: SELL " + sell.id +
                        " & BUY " + buy.id + " qty=" + traded);

                if (buy.remaining == 0) {
                    queue.poll();
                    orderMap.remove(buy.id);
                }
            }

            if (queue.isEmpty()) {
                buyMap.remove(bestBuyPrice);
                buyPrices.poll();
            }
        }
    }

    // =========================
    // Add to Book
    // =========================
    private void addToBook(Order order) {
        Map<Integer, Deque<Order>> map =
                order.type == OrderType.BUY ? buyMap : sellMap;

        PriorityQueue<Integer> heap =
                order.type == OrderType.BUY ? buyPrices : sellPrices;

        map.putIfAbsent(order.price, new ArrayDeque<>());

        if (!map.containsKey(order.price) || map.get(order.price).isEmpty()) {
            heap.offer(order.price);
        }

        map.get(order.price).offer(order);
    }

    // =========================
    // Cancel Order
    // =========================
    public void cancelOrder(String orderId) {
        Order order = orderMap.get(orderId);
        if (order == null) return;

        order.remaining = 0;
        orderMap.remove(orderId);

        System.out.println("CANCELLED: " + orderId);
    }

    // =========================
    // Best Prices
    // =========================
    public Integer getBestBid() {
        return buyPrices.isEmpty() ? null : buyPrices.peek();
    }

    public Integer getBestAsk() {
        return sellPrices.isEmpty() ? null : sellPrices.peek();
    }

    // =========================
    // Debug
    // =========================
    public void printBook() {
        System.out.println("BUY SIDE:");
        for (int price : buyPrices) {
            System.out.print(price + " -> ");
            for (Order o : buyMap.get(price)) {
                System.out.print(o.remaining + " ");
            }
            System.out.println();
        }

        System.out.println("SELL SIDE:");
        for (int price : sellPrices) {
            System.out.print(price + " -> ");
            for (Order o : sellMap.get(price)) {
                System.out.print(o.remaining + " ");
            }
            System.out.println();
        }
    }
	public static void main(String[] args) {
        OrderBook ob = new OrderBook();

        ob.addOrder(new Order("B1", OrderType.BUY, 100, 10));
        ob.addOrder(new Order("B2", OrderType.BUY, 101, 5));
        ob.addOrder(new Order("S1", OrderType.SELL, 100, 7));
        ob.addOrder(new Order("S2", OrderType.SELL, 101, 6));

        ob.printBook();

        ob.cancelOrder("B1");
    }
}