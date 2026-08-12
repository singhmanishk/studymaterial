class RateLimiter {

    private final int maxRequests;
    private final int windowSizeInMillis;

    private final ConcurrentHashMap<String, Deque<Long>> userMap;

    public RateLimiter(int maxRequests, int windowSizeInSeconds) {
        this.maxRequests = maxRequests;
        this.windowSizeInMillis = windowSizeInSeconds * 1000;
        this.userMap = new ConcurrentHashMap<>();
    }

    public boolean allowRequest(String userId) {
        long now = System.currentTimeMillis();

        userMap.putIfAbsent(userId, new ArrayDeque<>());
        Deque<Long> queue = userMap.get(userId);

        synchronized (queue) {

            // Remove old timestamps
            while (!queue.isEmpty() &&
                    now - queue.peekFirst() >= windowSizeInMillis) {
                queue.pollFirst();
            }

            if (queue.size() < maxRequests) {
                queue.offerLast(now);
                return true;
            } else {
                return false;
            }
        }
    }
	public static void main(String[] args) {
        RateLimiter limiter = new RateLimiter(5, 10);

        for (int i = 0; i < 7; i++) {
            System.out.println(limiter.allowRequest("user1"));
        }
    }
}