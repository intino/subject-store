# Subject History Format Reference

## Rows
Defines the temporal range of the view:

- `from`: start instant (e.g., `1800-01-01`)
- `to`: end instant (e.g., `2025-01-01`)
- `period`: duration of each time segment (e.g., `P1Y` for 1 year)

Dates can be defined with different levels of precision (year, month, day, etc.).

## Columns
List of columns to include in the view. Each column includes:

- `name`: name of the column
- `expr`: expression used to compute its value
- `filters` (optional): transformations applied to the expression result

Expressions define how column values are calculated and can refer to various types of variables. You can combine them using operators (`+`, `-`, `*`, `/`) and functions (`sin()`, `log10()`, etc.).

## Types of variables

- **Previous columns**: You can reference previously defined columns by name to reuse calculations.
- **Store tags**: Access time series stored in the subject's history. Use the format `tag.field`, e.g., `temperature.sum` or `visits.average`.
- **Time instants (`ts`)**: Extract calendar-based information from the interval timestamp, e.g., `ts.month-of-year`, `ts.day-of-week`.

## Tag Fields

Tags support field operators to aggregate values within a time segment. Syntax: `tag.field`. Example: `visits.sum`

| Operator       | Type        | Description                         |
|----------------|-------------|-------------------------------------|
| `count`        | Any         | Number of observations              |
| `sum`          | Numeric     | Total sum                           |
| `average`      | Numeric     | Arithmetic mean                     |
| `sd`,          | Numeric     | Standard deviation                  |
| `first`        | Numeric     | First observed value                |
| `last`         | Numeric     | Last observed value                 |
| `min`          | Numeric     | Minimum value                       |
| `max`          | Numeric     | Maximum value                       |
| `mode`         | Categorical | Most frequent value                 |
| `entropy`      | Categorical | Value diversity (dispersion)        |

## Time Fields

The `ts` object allows access to temporal markers:

| Field                            | Description                        |
|----------------------------------|------------------------------------|
| `day-of-week`                    | Day of the week (1–7)              |
| `day-of-month`                   | Day of the month (1–31)            |
| `month-of-year`                  | Month of the year (1–12)           |
| `quarter-of-year`                | Quarter (1–4)                      |
| `year`                           | Year                               |
| `year-quarter`                   | Formatted as `YYYYQX`             |
| `year-month`                     | Formatted as `YYYYMM`             |
| `year-month-day`                 | Formatted as `YYYYMMDD`           |
| `year-month-day-hour`            | Formatted as `YYYYMMDDHH`         |
| `year-month-day-hour-minute`     | Formatted as `YYYYMMDDHHmm`       |
| `year-month-day-hour-minute-second` | Formatted as `YYYYMMDDHHmmss`  |

## Functions

Expressions can include mathematical functions:

| Function    | Description         | Function | Description             |
|-------------|---------------------|----------|-------------------------|
| `abs`       | Absolute value       | `sin`    | Sine                    |
| `negate`    | Negative value (`-x`) | `cos`    | Cosine                  |
| `round`     | Round to nearest     | `tan`    | Tangent                 |
| `floor`     | Round down           | `asin`   | Arcsine                 |
| `ceil`      | Round up             | `acos`   | Arccosine               |
| `signum`    | Sign of a number     | `atan`   | Arctangent              |
| `exp`       | Exponential          | `sinh`   | Hyperbolic sine         |
| `log`       | Natural log (`ln`)   | `cosh`   | Hyperbolic cosine       |
| `log10`     | Base-10 log          | `tanh`   | Hyperbolic tangent      |
| `sqr`       | Square of a number   | `rad`    | Degrees to radians      |
| `sqrt`      | Square root          | `deg`    | Radians to degrees      |
| `cbrt`      | Cube root            |          |                         |

## Filters

Filters are transformations applied to numeric columns:

| Filter                     | Parameters     |
|----------------------------|----------------|
| `BinaryThreshold`          | `threshold`    |
| `CumulativeSum`            | --             |
| `Differencing`             | --             |
| `Lag`                      | `offset`       |
| `MinMaxNormalization`      | --             |
| `ZScoreNormalization`      | --             |
| `RollingAverage`           | `window`       |
| `RollingSum`               | `window`       |
| `RollingStandardDeviation` | `window`       |
| `RollingMax`               | `window`       |
| `RollingMin`               | `window`       |



