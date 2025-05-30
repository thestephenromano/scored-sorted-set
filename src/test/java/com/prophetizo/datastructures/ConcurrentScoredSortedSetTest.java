package com.prophetizo.datastructures;

import com.prophetizo.model.TestTick;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ConcurrentScoredSortedSet.
 *
 * Example usage:
 *   - For scoring with Integer
 *   - For scoring with Double
 */
public class ConcurrentScoredSortedSetTest {

    @Test
    public void testAddAndContains_IntegerScore() {
        // Create a set with Integer as score, String as element
        ConcurrentScoredSortedSet<Integer, String> set = new ConcurrentScoredSortedSet<>();

        set.add(10, "Alice");
        set.add(5, "Bob");

        assertTrue(set.contains("Alice"), "Set should contain 'Alice'");
        assertTrue(set.contains("Bob"),   "Set should contain 'Bob'");
        assertFalse(set.contains("Charlie"), "Set should not contain 'Charlie'");
        assertEquals(2, set.size(), "Size should be 2");

        // Check score retrieval
        assertEquals(10, set.getScore("Alice"));
        assertEquals(5,  set.getScore("Bob"));
        assertNull(set.getScore("Charlie"), "No score for an absent element");
    }

    @Test
    public void testAddAndContains_DoubleScore() {
        // Create a set with Double as score, Integer as element
        ConcurrentScoredSortedSet<Double, Integer> set = new ConcurrentScoredSortedSet<>();

        set.add(3.14, 100);
        set.add(2.718, 200);

        assertTrue(set.contains(100), "Set should contain key=100");
        assertTrue(set.contains(200), "Set should contain key=200");
        assertEquals(2.718, set.getScore(200));
        assertEquals(3.14,  set.getScore(100));
    }

    @Test
    public void testRemove() {
        ConcurrentScoredSortedSet<Integer, String> set = new ConcurrentScoredSortedSet<>();
        set.add(10, "Alice");
        set.add(5, "Bob");

        // Remove an existing element
        assertTrue(set.remove("Alice"));
        assertFalse(set.contains("Alice"));
        assertEquals(1, set.size());

        // Remove a non-existent element
        assertFalse(set.remove("Charlie"));
        assertEquals(1, set.size(), "Size should remain 1 after failing to remove");
    }

    @Test
    public void testRangeQuery() {
        ConcurrentScoredSortedSet<Integer, String> set = new ConcurrentScoredSortedSet<>();
        set.add(1,"Low");
        set.add(50, "Medium");
        set.add(100, "High");

        List<String> result = new ArrayList<>();
        set.rangeQuery(0, 60, result);  // should capture [Low, Medium]
        assertEquals(2, result.size());
        assertTrue(result.contains("Low"));
        assertTrue(result.contains("Medium"));

        result.clear();
        set.rangeQuery(50, 150, result); // should capture [Medium, High]
        assertEquals(2, result.size());
        assertTrue(result.contains("Medium"));
        assertTrue(result.contains("High"));

        result.clear();
        set.rangeQuery(2, 49, result);   // should capture nothing
        assertEquals(0, result.size());
    }

    @Test
    public void testMinMaxScore() {
        ConcurrentScoredSortedSet<Integer, String> set = new ConcurrentScoredSortedSet<>();
        assertNull(set.getMinScore(), "Empty set should return null for minScore");
        assertNull(set.getMaxScore(), "Empty set should return null for maxScore");

        set.add(10, "A");
        set.add(5, "B");
        set.add(100, "C");

        assertEquals(5,   set.getMinScore(), "Min should be 5");
        assertEquals(100, set.getMaxScore(), "Max should be 100");
    }

    @Test
    public void testDuplicateScores() {
        // Test scenario with same numeric score for different elements
        ConcurrentScoredSortedSet<Integer, String> set = new ConcurrentScoredSortedSet<>();
        set.add(10, "A");
        set.add(10, "B");
        set.add(10, "C");

        // All are contained
        assertTrue(set.contains("A"));
        assertTrue(set.contains("B"));
        assertTrue(set.contains("C"));
        // All have the same returned score (10)
        assertEquals(10, set.getScore("A"));
        assertEquals(10, set.getScore("B"));
        assertEquals(10, set.getScore("C"));
        // Size should be 3
        assertEquals(3, set.size());

        // Ensure range query picks them up
        List<String> result = new ArrayList<>();
        set.rangeQuery(10, 10, result);  // same min and max
        assertEquals(3, result.size());

        // Removing one should not affect the others
        assertTrue(set.remove("B"));
        assertFalse(set.contains("B"));
        assertEquals(2, set.size());
    }

    @Test
    public void testUpdateScore() {
        // Add an element, then re-add with a new score
        ConcurrentScoredSortedSet<Double, String> set = new ConcurrentScoredSortedSet<>();
        set.add(10.0, "Alice");
        set.add(20.0, "Alice");  // update to 20.0

        // Only 1 entry, new score
        assertEquals(1, set.size());
        assertEquals(20.0, set.getScore("Alice"));
    }

    @Test
    public void testConcurrency() throws InterruptedException {
        // A simple concurrency test: multiple threads add or remove elements
        final int THREAD_COUNT = 8;
        final int OPERATIONS_PER_THREAD = 1000;

        ConcurrentScoredSortedSet<Integer, Integer> set = new ConcurrentScoredSortedSet<>();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        // Each thread will do some mix of adds and removes
        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                    int element = (threadId * 100_000) + i;
                    // Add an element with a random score
                    set.add(element, i);
                    // Randomly remove some other element
                    if (i % 10 == 0) {
                        set.remove(element - 5); // might or might not exist
                    }
                }
            });
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Threads did not finish in time");

        // Basic check: the set should be consistent
        // We won't know exact final size, but let's ensure no exceptions occurred and that
        // membership queries behave as expected (no concurrency issues).
        for (Map.Entry<Integer, ConcurrentScoredSortedSet.ScoredKey<Integer>> entry : set.getIndexSnapshot().entrySet()) {
            Integer key = entry.getKey();
            ConcurrentScoredSortedSet.ScoredKey<Integer> scoredKey = entry.getValue();
            assertNotNull(scoredKey, "ScoredKey should not be null");
            assertTrue(set.contains(key), "Set should contain its known key");
        }
    }

    @Test
    void testRemoveAllLessThan_onEmptySet() {
        ConcurrentScoredSortedSet<Integer, String> set = new ConcurrentScoredSortedSet<>();
        assertEquals(0, set.size(), "Initial size should be 0");

        set.removeAllLessThan(10); // removing from empty set
        assertEquals(0, set.size(), "Size should remain 0 after removing any threshold from empty set");
    }

    @Test
    void testRemoveAllLessThan_allBelowThreshold() {
        ConcurrentScoredSortedSet<Integer, String> set = new ConcurrentScoredSortedSet<>();
        set.add(1, "A");
        set.add(2, "B");
        set.add(3, "C");

        assertEquals(3, set.size(), "Set should have 3 elements before removal");
        set.removeAllLessThan(10); // everything is less than 10
        assertEquals(0, set.size(), "All elements should be removed since they are all below threshold 10");
    }

    @Test
    void testRemoveAllLessThan_mixed() {
        // Setup: some scores below, some above, some exactly at threshold
        ConcurrentScoredSortedSet<Integer, String> set = new ConcurrentScoredSortedSet<>();
        set.add(1, "A"); // below
        set.add(2, "B"); // below
        set.add(2, "C"); // same score as B
        set.add(3, "D"); // threshold boundary
        set.add(5, "E"); // above

        assertEquals(5, set.size(), "Should have 5 elements initially");

        // Remove elements strictly less than 3
        set.removeAllLessThan(3);
        // Elements "A"(1) and "B","C"(2) should be removed.
        assertEquals(2, set.size(), "After removal, should have 2 elements left (scores >= 3)");
        assertFalse(set.contains("A"), "A should be removed");
        assertFalse(set.contains("B"), "B should be removed");
        assertFalse(set.contains("C"), "C should be removed");
        assertTrue(set.contains("D"), "D should remain because its score == 3");
        assertTrue(set.contains("E"), "E should remain because its score == 5");

        // Next removal: remove all below 5
        set.removeAllLessThan(5);
        // Now "D"(3) should be removed, leaving "E"(5).
        assertEquals(1, set.size(), "Only E should remain");
        assertFalse(set.contains("D"));
        assertTrue(set.contains("E"));
    }

    @Test
    void testRemoveAllLessThan_thresholdBelowAll() {
        // In this test, the threshold is below all elements,
        // so removeAllLessThan shouldn't remove anything.
        ConcurrentScoredSortedSet<Integer, String> set = new ConcurrentScoredSortedSet<>();
        set.add(10, "X");
        set.add(20, "Y");

        assertEquals(2, set.size(), "Should have 2 elements initially");

        // Everything is >= 10, so removing less than 5 won't remove anything
        set.removeAllLessThan(5);
        assertEquals(2, set.size(), "No elements should be removed if threshold < all scores");
        assertTrue(set.contains("X"));
        assertTrue(set.contains("Y"));
    }

    @Test
    void testRemoveAllLessThan_withDoubleScores() {
        // If you allow Double scores, ensure that removing based on double comparisons works.
        ConcurrentScoredSortedSet<Double, String> set = new ConcurrentScoredSortedSet<>();
        set.add(1.5, "A");
        set.add(2.0, "B");
        set.add(2.0, "C"); // duplicate score
        set.add(3.7, "D");

        // Now remove all less than 2.0
        set.removeAllLessThan(2.0);
        // "A"(1.5) is removed, "B"(2.0), "C"(2.0), and "D"(3.7) remain
        assertEquals(3, set.size(), "Three elements should remain after removing < 2.0");
        assertFalse(set.contains("A"));
        assertTrue(set.contains("B"));
        assertTrue(set.contains("C"));
        assertTrue(set.contains("D"));
    }

    @Test
    void testRemoveAllLessThan_afterMultipleAdds() {
        // Test scenario: re-adding the same element with different scores
        ConcurrentScoredSortedSet<Integer, String> set = new ConcurrentScoredSortedSet<>();

        set.add(5, "X"); // X has score 5
        set.add(1, "X"); // update X to score 1; old entry removed, new tie-breaker assigned
        set.add(2, "Y");
        set.add(10, "Z");

        // Current status: X(1), Y(2), Z(10)
        // Remove all less than 2
        set.removeAllLessThan(2);

        // X(1) is removed; Y(2), Z(10) remain
        assertEquals(2, set.size(), "Two elements should remain (Y, Z)");
        assertFalse(set.contains("X"));
        assertTrue(set.contains("Y"));
        assertTrue(set.contains("Z"));
    }

    @Test
    void loadTickDataSetTest(){

        ConcurrentScoredSortedSet<Long, TestTick> set = new ConcurrentScoredSortedSet<>();


        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("ticks_1734964200000L.csv")) {
            assert inputStream != null;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    // Split the line by comma
                    String[] data = line.split(",");
                    long timestamp = Long.parseLong(data[0]);
                    String side = data[1];
                    int volume = Integer.parseInt(data[2]);
                    float price = Float.parseFloat(data[3]);

                    // Create a tick object
                    TestTick tick = createMockTick(timestamp, price, volume, side.equals("ASK"));

                    // Add the tick to the set
                    set.add(timestamp, tick);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading test data", e);
        }
    }

    private TestTick createMockTick(long time, float price, int volume, boolean isAsk) {
        return new TestTick(time, price, volume, isAsk);
    }
}
