import java.util.*;

/*
Design a key-value store where each key can have multiple values over time, 
and you can query the value at a given timestamp.
*/

class TimeMap {

    private Map<String, List<Pair>> map;

    static class Pair {
        int timestamp;
        String value;

        Pair(int t, String v) {
            this.timestamp = t;
            this.value = v;
        }
    }

    public TimeMap() {
        map = new HashMap<>();
    }

    // =========================
    // Set
    // =========================
    public void set(String key, String value, int timestamp) {
        map.putIfAbsent(key, new ArrayList<>());
        map.get(key).add(new Pair(timestamp, value));
    }

    // =========================
    // Get
    // =========================
    public String get(String key, int timestamp) {
        if (!map.containsKey(key)) return "";

        List<Pair> list = map.get(key);

        int left = 0, right = list.size() - 1;
        String result = "";

        while (left <= right) {
            int mid = left + (right - left) / 2;

            if (list.get(mid).timestamp == timestamp) {
                return list.get(mid).value;
            }

            if (list.get(mid).timestamp < timestamp) {
                result = list.get(mid).value; // potential answer
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return result;
    }
	
	public static void main(String[] args) {
        TimeMap tm = new TimeMap();

        tm.set("foo", "bar", 1);
        tm.set("foo", "bar2", 4);

        System.out.println(tm.get("foo", 1)); // bar
        System.out.println(tm.get("foo", 3)); // bar
        System.out.println(tm.get("foo", 4)); // bar2
        System.out.println(tm.get("foo", 5)); // bar2
    }
}