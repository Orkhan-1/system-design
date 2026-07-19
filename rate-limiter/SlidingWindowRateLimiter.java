import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;

/*
Sliding Window Log

Idea:
Store timestamps of accepted requests.
Before every request:
1. Remove timestamps older than the window.
2. If queue size reaches the limit, reject.
3. Otherwise add the current timestamp.

Queue:
[100][250][500][650][900]

Request at 1100ms
Remove 100
Queue -> [250][500][650][900]

Time: O(1) amortized
Space: O(requests in window)
*/

public class SlidingWindowRateLimiter {

    private final int maxRequests;
    private final long windowMillis;

    private final ConcurrentHashMap<String, Deque<Long>> requests =
            new ConcurrentHashMap<>();

    public SlidingWindowRateLimiter(int maxRequests,
                                    long windowSeconds) {

        this.maxRequests = maxRequests;
        this.windowMillis = windowSeconds * 1000;
    }

    public boolean allowRequest(String clientId) {

        long now = System.currentTimeMillis();

        Deque<Long> queue =
                requests.computeIfAbsent(
                        clientId,
                        k -> new ArrayDeque<>());

        synchronized (queue) {

            while (!queue.isEmpty()
                    && queue.peekFirst() <= now - windowMillis) {
                queue.pollFirst();
            }

            if (queue.size() >= maxRequests) {
                return false;
            }

            queue.offerLast(now);
            return true;
        }
    }
}