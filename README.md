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

This snippet shows how to create a single subject, assign attributes, and track historical data over time:

```java
try (SubjectStore store = new SubjectStore("jdbc:sqlite:buildings.iss")) {
    Subject eiffel = store.create("eiffel-tower", "landmark").rename("eiffel tower");

    // Index the subject with static attributes
    eiffel.index()
        .set("name", "eiffel tower")
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

You can retrieve subjects either by their unique name and type, or through flexible queries based on their attributes.

```java
// Direct retrieval by name and type (namedType)
Subject subject = store.get("eiffel tower", "landmark");

// Check if a subject exists
boolean exists = store.has("taj mahal", "building");

// Querying by attribute match
Subject burj = store.subjects("building")
    .with("name", "burj khalifa")
    .first();

// Filtering using partial match or custom predicate
List<Subject> asianBuildings = store.subjects("building")
    .where("continent").contains("Asia")
    .all();

List<Subject> modernBuildings = store.subjects("building")
    .where("year").matches(v -> toNumber(v) > 1900)
    .all();

// Get all subjects of a given type or only root subjects
List<Subject> allBuildings = store.subjects("building").all();
List<Subject> rootSubjects = store.subjects().roots();