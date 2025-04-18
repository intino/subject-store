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

This snippet shows how to create a single subject and assign indexing attributes. The `index()` method is used to assign  static attributes intended for querying and identification, such as names, categories, or locations. 

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

Each subject in `SubjectStore` can record time-stamped historical data using the `history()` method. This feature allows tracking of evolving metrics, state changes, or temporal observations without altering the subject’s current indexed attributes.

Historical records are associated with both a date and a source (e.g.,`"sensor"`, `"website"`, `"manual"`), and can store arbitrary key-value pairs.

```java
try (SubjectHistory history = subject.history()) {
    history.on(LocalDate.of(2025, 4, 17), "website")
        .put("state", "open")
        .put("visitants", 3500)
        .put("income", 42000)
        .terminate();
}
```

Historical data can later be queried as typed signals and summarized over defined time periods:

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

## Managing Hierarchical Structures

`SubjectStore` allows subjects to be organized hierarchically by nesting child subjects under parents. The following example models a museum and its internal departments, focusing on structural identifiers.

```mermaid
graph TD
  A[National Museum]
  A1[Department of Art -- Floor 1]
  A2[Department of History -- Floor 2]
  A3[Department of Science -- Floor 3]
  A4[Fossil Collection -- Room 3B]

  A --> A1
  A --> A2
  A --> A3
  A3 --> 
```

```java
Subject museum = store.create("national-museum", "institution");

museum.index()
    .set("name", "National Museum")
    .put("city", "Washington D.C.")
    .put("country", "USA")
    .put("type", "cultural")
    .terminate();

Subject art = museum.create("art", "department");
art.index()
    .put("name", "Department of Art")
    .put("floor", 1)
    .terminate();

Subject history = museum.create("history", "department");
history.index()
    .put("name", "Department of History")
    .put("floor", 2)
    .terminate();

Subject science = museum.create("science", "department");
science.index()
    .put("name", "Department of Science")
    .put("floor", 3)
    .terminate();

Subject fossils = science.create("fossils", "section");
fossils.index()
    .put("name", "Fossil Collection")
    .put("room", "3B")
    .terminate();

```

### Hierarchy Structure {#hierarchy-structure .unnumbered}

-   **National Museum** (institution)

    -   **Department of Art** (department) -- Floor 1

    -   **Department of History** (department) -- Floor 2

    -   **Department of Science** (department) -- Floor 3

        -   **Fossil Collection** (section) -- Room 3B
