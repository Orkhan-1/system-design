import java.util.concurrent.ConcurrentHashMap;

/*
Leaky Bucket

Idea:
Requests fill a bucket with water.
Water leaks out at a constant rate.
Before each request:
1. Leak water based on elapsed time.
2. Reject if bucket overflows.
3. Otherwise add the request.

Incoming Requests
      ↓↓↓
 +------------+
 |   Water    |
 +------------+
       |
       | Constant leak
       V

Time: O(1)
Space: O(number of clients)
*/

public class LeakyBucketRateLimiter {

    static class Bucket {

        double water;
        long lastLeakTime;
    }

    private final int capacity;
    private final double leakRate;

    private final ConcurrentHashMap<String, Bucket> buckets =
            new ConcurrentHashMap<>();

    public LeakyBucketRateLimiter(
            int capacity,
            double leakRatePerSecond) {

        this.capacity = capacity;
        this.leakRate = leakRatePerSecond;
    }

    public boolean allowRequest(String clientId) {

        Bucket bucket =
                buckets.computeIfAbsent(clientId, k -> {
                    Bucket b = new Bucket();
                    b.lastLeakTime = System.currentTimeMillis();
                    return b;
                });

        synchronized (bucket) {

            long now = System.currentTimeMillis();

            double elapsed =
                    (now - bucket.lastLeakTime) / 1000.0;

            bucket.water = Math.max(
                    0,
                    bucket.water - elapsed * leakRate);

            bucket.lastLeakTime = now;

            if (bucket.water + 1 > capacity)
                return false;

            bucket.water++;

            return true;
        }
    }
}