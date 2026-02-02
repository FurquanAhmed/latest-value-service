package com.snp.latestvalue;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * LatestValueService
 *
 * This class is the service interface and implementation in a single Java class.
 *
 *
 * Design notes:
 *  - committed snapshots are stored in an AtomicReference to an immutable Map. Swapping the reference
 *    guarantees atomic visibility of an entire batch upon commit.
 *  - active batches are stored in a ConcurrentHashMap of id -> PriceRecord
 *    representing the best record within that batch.
 *  - chunk uploads merge into the batch map comparing asOf.
 *  - All public methods are thread-safe.
 *
 * PriceRecord is an immutable POJO nested for convenience.
 */
public class LatestValueService {

    public static final class PriceRecord {
        private final String id;
        private final Instant asOf;
        private final Map<String, Object> payload;

        public PriceRecord(String id, Instant asOf, Map<String, Object> payload) {
            if (id == null || asOf == null) throw new IllegalArgumentException("id and asOf required");
            this.id = id;
            this.asOf = asOf;
            // shallow copy 
            this.payload = payload == null ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(payload));
        }

        public String getId() { return id; }
        public Instant getAsOf() { return asOf; }
        public Map<String, Object> getPayload() { return payload; }

        public PriceRecord withPayload(Map<String, Object> newPayload) {
            return new PriceRecord(id, asOf, newPayload);
        }

        @Override
        public String toString() {
            return "PriceRecord{" +
                    "id='" + id + '\'' +
                    ", asOf=" + asOf +
                    ", payload=" + payload +
                    '}';
        }
    }

    // Represent batch states 
    private static final class Batch {
        final ConcurrentHashMap<String, PriceRecord> map = new ConcurrentHashMap<>();
    }

    // Active batches 
    private final ConcurrentHashMap<String, Batch> batches = new ConcurrentHashMap<>();

    // Committed snapshot
    private final AtomicReference<Map<String, PriceRecord>> committed = new AtomicReference<>(Collections.emptyMap());

    /**
     * Start a batch. 
     */
    public void startBatch(String batchId) {
        Objects.requireNonNull(batchId, "batchId required");
        batches.computeIfAbsent(batchId, k -> new Batch());
    }

    /**
     * Upload a chunk of records for given batchId. 
     */
    public void uploadChunk(String batchId, Collection<PriceRecord> records) {
        Objects.requireNonNull(batchId, "batchId required");
        Objects.requireNonNull(records, "records required");
        Batch batch = batches.computeIfAbsent(batchId, k -> new Batch());

        // For each record, merge into batch map keeping the record with latest asOf for that id inside the batch
        for (PriceRecord r : records) {
            if (r == null) continue;
            batch.map.merge(r.getId(), r, (existing, incoming) -> {
                // choose later asOf
                return incoming.getAsOf().isAfter(existing.getAsOf()) ? incoming : existing;
            });
        }
    }

    /**
     * Complete the batch:
     * If multiple threads attempt to complete the same batch concurrently, only the first will effectively commit.
     */
    public void completeBatch(String batchId) {
        Objects.requireNonNull(batchId, "batchId required");
        Batch batch = batches.remove(batchId);
        if (batch == null) return; 

        Map<String, PriceRecord> before;
        Map<String, PriceRecord> after;
        while (true) {
            before = committed.get();
            // shallow copy
            HashMap<String, PriceRecord> copy = new HashMap<>(before);

            // Apply batch updates
            for (Map.Entry<String, PriceRecord> e : batch.map.entrySet()) {
                String id = e.getKey();
                PriceRecord candidate = e.getValue();
                PriceRecord existing = copy.get(id);
                if (existing == null || candidate.getAsOf().isAfter(existing.getAsOf())) {
                    copy.put(id, candidate);
                }
            }
            after = Collections.unmodifiableMap(copy);
            // Attempt to CAS the committed snapshot
            if (committed.compareAndSet(before, after)) {
                break;
            }
            // else
        }
    }

    /**
     * Cancel batch
     */
    public void cancelBatch(String batchId) {
        Objects.requireNonNull(batchId, "batchId required");
        batches.remove(batchId);
    }

    /**
     * Consumer API
     */
    public Map<String, PriceRecord> getLatest(Collection<String> ids) {
        Objects.requireNonNull(ids, "ids required");
        Map<String, PriceRecord> snap = committed.get();
        HashMap<String, PriceRecord> out = new HashMap<>();
        for (String id : ids) {
            PriceRecord r = snap.get(id);
            if (r != null) out.put(id, r);
        }
        return out;
    }

    public Map<String, PriceRecord> getAllCommitted() {
        return committed.get();
    }
}
