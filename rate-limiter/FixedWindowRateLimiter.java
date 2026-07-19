import java.util.concurrent.ConcurrentHashMap;

/*
Fixed Window Counter

Idea:
Divide time into fixed windows (e.g., 1 second). Count requests in the
current window. When a new window starts, reset the counter.

Example (limit = 5/sec)

Window 1        Window 2
|-----------|-----------|
1 2 3 4 5 X 1 2 3

Time: O(1)
Space: O(number of clients)
*/

public class FixedWindowRateLimiter {

    private static class Window {
        long startTime;
        int count;

        Window(long startTime) {
            this.startTime = startTime;
        }
    }

    private final int maxRequests;
    private final long windowMillis;
    private final ConcurrentHashMap<String, Window> windows =
            new ConcurrentHashMap<>();

    public FixedWindowRateLimiter(int maxRequests, long windowSeconds) {
        this.maxRequests = maxRequests;
        this.windowMillis = windowSeconds * 1000;
    }

    public boolean allowRequest(String clientId) {

        long now = System.currentTimeMillis();

        Window window = windows.computeIfAbsent(
                clientId,
                k -> new Window(now));

        synchronized (window) {

            if (now - window.startTime >= windowMillis) {
                window.startTime = now;
                window.count = 0;
            }

            if (window.count >= maxRequests) {
                return false;
            }

            window.count++;
            return true;
        }
    }
}