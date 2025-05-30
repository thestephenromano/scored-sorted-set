# ConcurrentScoredSortedSet

A thread-safe, scored, and sorted set implementation for Java, supporting generic numeric scores and efficient concurrent operations.

## Features

- **Thread-safe**: Uses concurrent data structures for safe multi-threaded access.
- **Generic scores**: Supports any `Number` type (e.g., `Integer`, `Double`, `Long`).
- **Sorted order**: Maintains elements in ascending order by score.
- **Duplicate scores**: Allows multiple elements with the same score, using a unique tie-breaker for insertion order.
- **Efficient operations**: Fast add, remove, contains, and range queries.
- **Comprehensive tests**: Includes unit tests for all major behaviors.

## Usage

### Add Elements

```java
ConcurrentScoredSortedSet<Integer, String> set = new ConcurrentScoredSortedSet<>();
set.add(10, "A");
set.add(5, "B");
set.add(10, "C"); // Duplicate score allowed