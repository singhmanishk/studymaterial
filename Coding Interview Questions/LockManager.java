class Lock {
    String resourceId;
    String clientId;
    long expiryTime;
}

Map<String, Lock> lockMap;

public synchronized boolean lock(String resourceId, String clientId, long ttlMillis) {
    long now = System.currentTimeMillis();

    if (!lockMap.containsKey(resourceId)) {
        lockMap.put(resourceId, new Lock(resourceId, clientId, now + ttlMillis));
        return true;
    }

    Lock existing = lockMap.get(resourceId);

    // If expired → allow takeover
    if (existing.expiryTime < now) {
        lockMap.put(resourceId, new Lock(resourceId, clientId, now + ttlMillis));
        return true;
    }

    return false; // already locked
}

public synchronized boolean unlock(String resourceId, String clientId) {
    Lock lock = lockMap.get(resourceId);

    if (lock != null && lock.clientId.equals(clientId)) {
        lockMap.remove(resourceId);
        return true;
    }

    return false;
}