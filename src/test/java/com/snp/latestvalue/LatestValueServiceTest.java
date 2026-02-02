package com.snp.latestvalue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class LatestValueServiceTest {

    private LatestValueService svc;

    @BeforeEach
    void before() {
        svc = new LatestValueService();
    }

    @Test
    void testSimpleBatchCommitVisibility() {
        String batch = "b1";
        svc.startBatch(batch);
        LatestValueService.PriceRecord r1 = new LatestValueService.PriceRecord("A", Instant.parse("2025-01-01T10:00:00Z"), Map.of("p", 1));
        svc.uploadChunk(batch, List.of(r1));

        // Before complete, consumer should not see the record
        assertTrue(svc.getLatest(List.of("A")).isEmpty());

        svc.completeBatch(batch);

        Map<String, LatestValueService.PriceRecord> got = svc.getLatest(List.of("A"));
        assertEquals(1, got.size());
        assertEquals(r1.getAsOf(), got.get("A").getAsOf());
    }

    @Test
    void testCancelBatch() {
        String batch = "b2";
        svc.startBatch(batch);
        svc.uploadChunk(batch, List.of(new LatestValueService.PriceRecord("X", Instant.now(), Map.of("v", 1))));
        svc.cancelBatch(batch);
        assertTrue(svc.getLatest(List.of("X")).isEmpty());
    }

    @Test
    void testLaterAsOfWinsOnCommit() {
        String batch1 = "b1";
        String batch2 = "b2";

        svc.startBatch(batch1);
        svc.uploadChunk(batch1, List.of(new LatestValueService.PriceRecord("ID", Instant.parse("2024-01-01T00:00:00Z"), Map.of("v", 1))));
        svc.completeBatch(batch1);

        svc.startBatch(batch2);
        svc.uploadChunk(batch2, List.of(new LatestValueService.PriceRecord("ID", Instant.parse("2025-01-01T00:00:00Z"), Map.of("v", 2))));
        svc.completeBatch(batch2);

        var r = svc.getLatest(List.of("ID")).get("ID");
        assertNotNull(r);
        assertEquals(Instant.parse("2025-01-01T00:00:00Z"), r.getAsOf());
        assertEquals(Map.of("v",2), r.getPayload());
    }

    @Test
    void testParallelChunkUploadsWithinBatch() throws Exception {
        String batch = "big-batch";
        svc.startBatch(batch);

        int totalIds = 5000;
        int chunkSize = 1000; // as per spec
        List<String> ids = IntStream.range(0, totalIds).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        ExecutorService ex = Executors.newFixedThreadPool(8);
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int i = 0; i < totalIds; i += chunkSize) {
            final int start = i;
            tasks.add(() -> {
                List<LatestValueService.PriceRecord> chunk = new ArrayList<>();
                for (int j = start; j < Math.min(start + chunkSize, totalIds); j++) {
                    String id = ids.get(j);
                    chunk.add(new LatestValueService.PriceRecord(id, Instant.now(), Map.of("v", j)));
                }
                svc.uploadChunk(batch, chunk);
                return null;
            });
        }

        ex.invokeAll(tasks);
        ex.shutdown();
        ex.awaitTermination(5, TimeUnit.SECONDS);

        // still not visible
        assertTrue(svc.getLatest(ids).isEmpty());

        svc.completeBatch(batch);

        Map<String, LatestValueService.PriceRecord> committed = svc.getLatest(ids);
        assertEquals(totalIds, committed.size());
    }

    @Test
    void testResilienceToMisorderedCalls() {
        String batch = "implicit";

        // Upload without explicit start -> should create batch implicitly
        svc.uploadChunk(batch, List.of(new LatestValueService.PriceRecord("A", Instant.now(), Map.of("k", "v"))));
        // Now complete
        svc.completeBatch(batch);
        assertTrue(svc.getLatest(List.of("A")).containsKey("A"));

        // Cancel after complete -> no effect
        svc.cancelBatch(batch);
    }

}
