# SubjectStore

**SubjectStore** is a lightweight Java library for building, indexing, and querying hierarchical entities called **subjects**. Each subject can hold structured attributes, contain child subjects, and optionally maintain a time-stamped history of its state or metrics.

It is designed for applications that need both a flexible data model and a lightweight, embeddable query engine — such as knowledge bases, simulations, digital twins, domain models, or semantic data layers.

## Features

- Hierarchical subjects with typed attributes
- Fluent creation and update API
- Expressive query interface (`with`, `where`, `matches`, `contains`)
- Support for time-series historical data per subject
- Lightweight and fast

---

## Quick Example

This snippet shows how to create a single subject and assign indexing attributes.

```java
try (SubjectStore store = new SubjectStore("jdbc:sqlite:buildings.iss")) {
    Subject eiffel = store.create("eiffel tower", "building");

    // Index the subject with static attributes
    eiffel.index()
        .set("name", "Eiffel Tower")
        .set("year", 1889)
        .put("city", "Paris")
        .put("country", "France")
        .put("continent", "Europe")
        .terminate();

    // Record a historical snapshot
    try (SubjectHistory history = eiffel.history()) {
        history.on(LocalDate.now(), "visitor-data")
            .put("state", "open")
            .put("visitants", 3500)
            .put("income", 42000)
            .terminate();
    }
}
```

## Retrieving Subjects

You can retrieve subjects either by their unique name and type, or through flexible queries based on their indexed attributes.

```java
// Check if a subject exists
boolean exists = store.has("taj mahal", "building");

// Direct retrieval by name and type (namedType)
Subject eiffel = store.get("eiffel tower", "building");

// Querying by attribute match
Subject eiffel = store.subjects("building")
    .with("city", "Paris")
    .first();

// Filtering using partial match or custom predicate
Subjects towers = store.subjects("building")
    .where("name").contains("tower")
    .all();

Subjects modernBuildings = store.subjects("building")
    .where("year").matches(v -> toNumber(v) > 1900)
    .all();

```

## Tracking Historical Data

Each subject in `SubjectStore` can record time-stamped historical data
using the `.history()` API. This feature allows tracking of evolving
metrics, state changes, or temporal observations without altering the
subject’s current indexed attributes.

Historical records are associated with both a date and a source (e.g.,
`"sensor"`, `"website"`, `"manual"`), and can store arbitrary key-value
pairs.

``` java
try (SubjectHistory history = subject.history()) {
    history.on(LocalDate.of(2025, 4, 17), "website")
        .put("state", "open")
        .put("visitants", 3500)
        .put("income", 42000)
        .terminate();
}
```

Historical data can later be queried as typed signals and summarized
over defined time periods:

``` java
try (SubjectHistory history = subject.history()) {
    // Numeric signal: visitants in the last 30 days
    NumericalSignal visitants = history.query()
        .number("visitants")
        .get(TimeSpan.LastMonth);

    // Average visitants in the last 30 days
    double average = visitants.summary().mean();

    // Categorical signal: states in this year
    CategoricalSignal states = history.query()
        .text("state")
        .get(LocalDate.of(2025, 1, 1), LocalDate.now());
        
    // Most frequent state in this year
    String state = states.summary().mode();
}
```

