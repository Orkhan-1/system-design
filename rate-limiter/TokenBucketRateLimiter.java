import java.util.concurrent.ConcurrentHashMap;

/*
Token Bucket

Idea:
Bucket starts full of tokens.
Tokens are refilled at a constant rate.
Each request consumes one token.
Reject if no token is available.

Capacity = 10
Refill = 2 tokens/sec

10 tokens
↓
5 requests
↓
5 tokens

Wait 2 sec
↓
+4 tokens
↓
9 tokens

Time: O(1)
Space: O(number of clients)
*/

public class TokenBucketRateLimiter {

    static class Bucket {

        double tokens;
        long lastRefillTime;

        Bucket(int capacity) {
            tokens = capacity;
            lastRefillTime = System.currentTimeMillis();
        }
    }

    private final int capacity;
    private final double refillRate;

    private final ConcurrentHashMap<String, Bucket> buckets =
            new ConcurrentHashMap<>();

    public TokenBucketRateLimiter(
            int capacity,
            double refillRatePerSecond) {

        this.capacity = capacity;
        this.refillRate = refillRatePerSecond;
    }

    public boolean allowRequest(String clientId) {

        Bucket bucket =
                buckets.computeIfAbsent(
                        clientId,
                        k -> new Bucket(capacity));

        synchronized (bucket) {

            long now = System.currentTimeMillis();

            double elapsed =
                    (now - bucket.lastRefillTime) / 1000.0;

            bucket.tokens = Math.min(
                    capacity,
                    bucket.tokens + elapsed * refillRate);

            bucket.lastRefillTime = now;

            if (bucket.tokens < 1)
                return false;

            bucket.tokens--;

            return true;
        }
    }
}