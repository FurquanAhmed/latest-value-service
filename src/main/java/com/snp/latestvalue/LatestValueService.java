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
 * API methods:
 *  - void startBatch(String batchId)
 *  - void uploadChunk(String batchId, List<PriceRecord> records)
 *  - void completeBatch(String batchId)
 *  - void cancelBatch(String batchId)
 *  - Map<String, PriceRecord> getLatest(Collection<String> ids)
 *
 * Design notes:
 *  - committed snapshots are stored in an AtomicReference to an immutable Map. Swapping the reference
 *    guarantees atomic visibility of an entire batch upon commit.
 *  - active batches are stored in a ConcurrentHashMap. Each Batch keeps a ConcurrentHashMap of id -> PriceRecord
 *    representing the best (latest-asOf) record within that batch.
 *  - chunk uploads merge into the batch map using compute(...) comparing asOf.
 *  - All public methods are thread-safe.
 *
 * PriceRecord is an immutable POJO nested for convenience.
 */
public class LatestValueService {

    /**
     * Public immutable price record.
     */
    public static final class PriceRecord {
        private final String id;
        private final Instant asOf;
        private final Map<String, Object> payload;

        public PriceRecord(String id, Instant asOf, Map<String, Object> payload) {
            if (id == null || asOf == null) throw new IllegalArgumentException("id and asOf required");
            this.id = id;
            this.asOf = asOf;
            // shallow copy to encourage immutability (not defensive deep copy for simplicity)
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

    // Represent batch states simply: PENDING until completed or cancelled.
    private static final class Batch {
        final ConcurrentHashMap<String, PriceRecord> map = new ConcurrentHashMap<>();
    }

    // Active batches keyed by batchId
    private final ConcurrentHashMap<String, Batch> batches = new ConcurrentHashMap<>();

    // Committed snapshot: id -> PriceRecord (the latest committed)
    private final AtomicReference<Map<String, PriceRecord>> committed = new AtomicReference<>(Collections.emptyMap());

    /**
     * Start a batch. If batch already exists, this is a no-op.
     * Resilient: calling start multiple times is safe.
     */
    public void startBatch(String batchId) {
        Objects.requireNonNull(batchId, "batchId required");
        batches.computeIfAbsent(batchId, k -> new Batch());
    }

    /**
     * Upload a chunk of records for given batchId. Chunks are merged in parallel safely.
     * If the batch does not exist we create it implicitly (resilience).
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
     * Complete the batch: atomically make all prices in the batch visible to consumers.
     * If batch doesn't exist, this is a no-op (resilient).
     * If multiple threads attempt to complete the same batch concurrently, only the first will effectively commit.
     */
    public void completeBatch(String batchId) {
        Objects.requireNonNull(batchId, "batchId required");
        Batch batch = batches.remove(batchId);
        if (batch == null) return; // nothing to complete

        // Create a new committed map by copying current committed and applying updates from the batch.
        // This is computed locally and then swapped atomically into committed reference. This guarantees
        // that consumers see either the entire previous state or the entire new state, never a partial application.
        Map<String, PriceRecord> before;
        Map<String, PriceRecord> after;
        while (true) {
            before = committed.get();
            // shallow copy: new HashMap based on previous committed snapshot
            HashMap<String, PriceRecord> copy = new HashMap<>(before);

            // Apply batch updates: for each id in batch, choose the later of (existing committed, batch)
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
            // else: some other thread committed concurrently; retry with new 'before' snapshot
        }
    }

    /**
     * Cancel batch: discard any uploaded but uncommitted data. If batch doesn't exist, no-op.
     */
    public void cancelBatch(String batchId) {
        Objects.requireNonNull(batchId, "batchId required");
        batches.remove(batchId);
    }

    /**
     * Consumer API: get the latest (committed) records for given ids.
     * Returns a map id -> PriceRecord containing only entries that are present in committed snapshot.
     * Consumers will never see uncommitted/partial batches because they only read the committed snapshot.
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

    /**
     * Convenience: get all committed entries (for tests/debug)
     */
    public Map<String, PriceRecord> getAllCommitted() {
        return committed.get();
    }
}
