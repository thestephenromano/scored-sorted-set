package com.prophetizo.datastructures;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A thread-safe scored set where the score is a generic numeric type (N extends Number).
 * Duplicate scores are allowed via a unique tie-breaker.
 * <p>
 * The set is maintained in ascending order by converting the score to a double
 * for comparison. If you store both Integers and Doubles (mixed), they are all
 * compared by their double value.
 */
public class ConcurrentScoredSortedSet<N extends Number, T> {

    /**
     * A global counter to generate strictly increasing tie-breakers.
     */
    private final AtomicLong tieBreakerGenerator = new AtomicLong(0L);
    /**
     * The core skip list mapping from ScoredKey -> T.
     */
    private final ConcurrentSkipListMap<ScoredKey<N>, T> skipListMap;
    /**
     * A secondary index for O(1) membership and to find the existing key for an element.
     * T -> ScoredKey
     */
    private final ConcurrentMap<T, ScoredKey<N>> index;

    public ConcurrentScoredSortedSet() {
        this.skipListMap = new ConcurrentSkipListMap<>();
        this.index = new ConcurrentHashMap<>();
    }

    /**
     * Insert or update an element with the given score.
     * If the element already exists, we remove the old entry and insert a new one
     * (thus getting a new tie-breaker).
     */
    public void add(N score, T element) {
        // Remove existing key if present
        ScoredKey<N> oldKey = index.remove(element);
        if (oldKey != null) {
            skipListMap.remove(oldKey);
        }

        // Generate a new tie-breaker
        long tb = tieBreakerGenerator.incrementAndGet();
        ScoredKey<N> newKey = new ScoredKey<>(score, tb);

        skipListMap.put(newKey, element);
        index.put(element, newKey);
    }

    /**
     * Remove an element if present.
     *
     * @return true if the element was found/removed, false otherwise
     */
    public boolean remove(T element) {
        ScoredKey<N> key = index.remove(element);
        if (key == null) {
            return false;  // not in the set
        }
        skipListMap.remove(key);
        return true;
    }

    /**
     * Check if an element is in this set.
     */
    public boolean contains(T element) {
        return index.containsKey(element);
    }

    /**
     * Return the total number of elements in this set.
     */
    public int size() {
        return skipListMap.size();
    }

    /**
     * Get the current score of the element (as the original numeric type),
     * or null if the element is not present.
     */
    public N getScore(T element) {
        ScoredKey<N> key = index.get(element);
        return (key != null) ? key.rawScore : null;
    }

    /**
     * Range query: find all elements with scores in [minScore, maxScore].
     * This uses double comparisons for bounding.
     */
    public void rangeQuery(N minScore, N maxScore, List<T> result) {
        // Create bounding keys that capture all possible tie-breakers
        ScoredKey<N> lowerBound = new ScoredKey<>(minScore, Long.MIN_VALUE);
        ScoredKey<N> upperBound = new ScoredKey<>(maxScore, Long.MAX_VALUE);

        NavigableMap<ScoredKey<N>, T> subMap = skipListMap.subMap(lowerBound, true, upperBound, true);
        for (Map.Entry<ScoredKey<N>, T> e : subMap.entrySet()) {
            result.add(e.getValue());
        }
    }

    /**
     * Return a read-only snapshot of the internal index.
     * Useful for debugging or testing concurrency.
     */
    public Map<T, ScoredKey<N>> getIndexSnapshot() {
        return Collections.unmodifiableMap(new HashMap<>(index));
    }

    /**
     * Return the smallest (lowest) score, or null if empty.
     * The returned type is the same as the generic N (could be Integer, Double, etc.).
     */
    public N getMinScore() {
        Map.Entry<ScoredKey<N>, T> first = skipListMap.firstEntry();
        return (first != null) ? first.getKey().rawScore : null;
    }

    /**
     * Return the largest (highest) score, or null if empty.
     */
    public N getMaxScore() {
        Map.Entry<ScoredKey<N>, T> last = skipListMap.lastEntry();
        return (last != null) ? last.getKey().rawScore : null;
    }

    /**
     * Remove all elements whose score is strictly less than the given threshold.
     *
     * @param threshold The score threshold; all scores strictly below this value will be removed.
     */
    public void removeAllLessThan(N threshold) {
        // Construct a boundary key that starts at 'threshold' with the lowest possible tie-breaker
        // so that "less than threshold" entries lie strictly below this key.
        ScoredKey<N> boundary = new ScoredKey<>(threshold, Long.MIN_VALUE);

        // Create a sub-map view of all keys strictly below the boundary.
        // false in the second parameter excludes the boundary key from the submap.
        NavigableMap<ScoredKey<N>, T> headMap = skipListMap.headMap(boundary, false);

        // We need to remove entries from both skipListMap (via headMap) and the index map.
        // First, remove them from the index by iterating.
        for (Map.Entry<ScoredKey<N>, T> entry : headMap.entrySet()) {
            index.remove(entry.getValue());
        }
        // Now, clear the sub-map, which removes those entries from skipListMap.
        headMap.clear();
    }

    /**
     * Composite key: (numericScore, tieBreaker, rawScore).
     * - numericScore: double value for sorting.
     * - tieBreaker: unique insertion order for stable ordering when scores are equal.
     * - rawScore: original numeric type N for reference.
     */
    public static class ScoredKey<N extends Number> implements Comparable<ScoredKey<N>> {
        final double numericScore;   // used for sorting comparisons
        final long tieBreaker;       // ensures uniqueness for duplicate numericScore
        final N rawScore;            // original numeric score (Integer, Double, etc.)

        ScoredKey(N rawScore, long tieBreaker) {
            this.rawScore = rawScore;
            this.tieBreaker = tieBreaker;
            // Convert to double for consistent sorting
            this.numericScore = rawScore.doubleValue();
        }

        @Override
        public int compareTo(ScoredKey<N> other) {
            // Compare by numericScore (double)
            int cmp = Double.compare(this.numericScore, other.numericScore);
            if (cmp != 0) {
                return cmp;
            }
            // If scores are equal, compare by tieBreaker
            return Long.compare(this.tieBreaker, other.tieBreaker);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ScoredKey<?> other = (ScoredKey<?>) obj;
            // Compare numericScore as bits plus tieBreaker
            return Double.doubleToLongBits(this.numericScore) == Double.doubleToLongBits(other.numericScore)
                    && this.tieBreaker == other.tieBreaker;
        }

        @Override
        public int hashCode() {
            int hash = Double.hashCode(this.numericScore);
            hash = 31 * hash + Long.hashCode(tieBreaker);
            return hash;
        }

        @Override
        public String toString() {
            return "(" + rawScore + ", tie=" + tieBreaker + ")";
        }
    }
}
