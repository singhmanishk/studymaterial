import java.util.concurrent.locks.*;

class TokenBucketRateLimiter {

    private final long capacity;
    private final long refillRatePerSecond;

    private double tokens;
    private long lastRefillTime;

    private final Lock lock = new ReentrantLock();

    public TokenBucketRateLimiter(long capacity, long refillRatePerSecond) {
        this.capacity = capacity;
        this.refillRatePerSecond = refillRatePerSecond;
        this.tokens = capacity;
        this.lastRefillTime = System.nanoTime();
    }

    public boolean allowRequest() {
        lock.lock();
        try {
            refillTokens();

            if (tokens >= 1) {
                tokens -= 1;
                return true;
            }
            return false;

        } finally {
            lock.unlock();
        }
    }

    private void refillTokens() {
        long now = System.nanoTime();

        double elapsedSeconds = (now - lastRefillTime) / 1_000_000_000.0;

        double tokensToAdd = elapsedSeconds * refillRatePerSecond;

        tokens = Math.min(capacity, tokens + tokensToAdd);

        lastRefillTime = now;
    }
	
	    public static void main(String[] args) throws InterruptedException {
        TokenBucketRateLimiter limiter =
                new TokenBucketRateLimiter(5, 1); // 5 tokens, 1/sec

        for (int i = 0; i < 10; i++) {
            System.out.println("Request " + i + ": " + limiter.allowRequest());
            Thread.sleep(200);
        }
    }
}