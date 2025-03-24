package io.intino.alexandria;

import io.intino.alexandria.model.table.Column.Categorical;
import io.intino.alexandria.model.table.Column.Numerical;
import io.intino.alexandria.model.table.Column.Temporal;
import io.intino.alexandria.model.table.Format;
import io.intino.alexandria.model.table.Column;
import io.intino.alexandria.model.series.Sequence;
import io.intino.alexandria.model.series.Signal;

import java.io.*;
import java.time.*;
import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public class SubjectTable implements Iterable<SubjectTable.Row> {
	private final Format format;
	private final Row[] rows;
	private final SubjectStore store;

	public SubjectTable(SubjectStore store, Format format) {
		this.store = store;
		this.format = format;
		this.rows = createRows();
		this.build();
	}

	public List<Column> columns() {
		return format.columns();
	}

	public Instant from() {
		return format.from();
	}

	public Instant to() {
		return format.to();
	}

	public Duration duration() {
		return format.duration();
	}

	public Row[] rows() {
		return rows;
	}

	public void export(File file) throws IOException {
		try (OutputStream os = new FileOutputStream(file)) {
			export(os);
		}
	}

	public void export(OutputStream os) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
			writer.write(tsv());
		}
	}

	private void build() {
		columns().forEach(this::build);
	}

	private void build(Column column) {
		switch (column.type()) {
			case Temporal -> build((Temporal) column);
			case Numerical -> build((Numerical) column);
			case Categorical -> build((Categorical) column);
		}
	}

	private void build(Temporal column) {
		for (Row row : rows)
			row.add(column.operator().apply(row.instant));
	}

	private void build(Numerical column) {
		Signal signal = store.numericalQuery(column.name()).signal(from(), to());
		build(signal.segments(duration()), column);
	}

	private void build(Categorical column) {
		Sequence sequence = store.categoricalQuery(column.name()).sequence(from(), to());
		build(sequence.segments(duration()), column);
	}

	private void build(Signal[] signals, Numerical column) {
		for (int i = 0; i < rows.length; i++)
			rows[i].add(column.apply(signals[i]));
	}

	private void build(Sequence[] sequences, Categorical column) {
		for (int i = 0; i < rows.length; i++)
			rows[i].add(column.apply(sequences[i]));
	}

	private String tsv() {
		return Arrays.stream(rows)
				.map(this::tsv)
				.collect(joining("\n"));
	}

	private String tsv(Row row) {
		return row.values().stream()
				.map(this::tsv)
				.collect(joining("\t"));
	}

	private String tsv(Object value) {
		return value != null ? value.toString() : "";
	}

	private Row[] createRows() {
		return instants()
				.map(Row::new)
				.toArray(Row[]::new);
	}

	private Stream<Instant> instants() {
		return Stream.iterate(
				format.from(),
				instant -> instant.isBefore(format.to()),
				instant -> instant.plus(format.duration())
		);
	}

	@Override
	public String toString() {
		return "Table(" + format + ")";
	}

	@Override
	public Iterator<Row> iterator() {
		return Arrays.asList(rows).iterator();
	}

	public SubjectTable normalize(int column) {
		normalize(column, rangeOf(column));
		return this;
	}

	private void normalize(int column, double range) {
		for (Row row : rows) {
			Object object = row.values.get(column);
			if (object instanceof Number number) {
				double value = number.doubleValue();
				row.values.set(column, value / range);
			}
		}
	}

	private double rangeOf(int i) {
		double min = Double.POSITIVE_INFINITY;
		double max = Double.NEGATIVE_INFINITY;
		for (Row row : rows) {
			Object object = row.values.get(i);
			if (object instanceof Number number) {
				double value = number.doubleValue();
				min = Math.min(min, value);
				max = Math.max(max, value);
			}
		}
		return max - min;
	}

	public static class Row {
		private final Instant instant;
		private final List<Object> values;

		public Row(Instant instant) {
			this.instant = instant;
			this.values = new ArrayList<>();
		}

		public Instant instant() {
			return instant;
		}

		public List<Object> values() {
			return values;
		}

		public void add(Object value) {
			values.add(value);
		}

		@Override
		public String toString() {
			return "Row(" + instant + ")";
		}
	}

}
