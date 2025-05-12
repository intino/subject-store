package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.calculator.VectorCalculator;
import systems.intino.datamarts.subjectstore.calculator.model.Filter;
import systems.intino.datamarts.subjectstore.calculator.model.Vector;
import systems.intino.datamarts.subjectstore.calculator.model.vectors.DoubleVector;
import systems.intino.datamarts.subjectstore.calculator.model.vectors.StringVector;
import systems.intino.datamarts.subjectstore.model.signals.CategoricalSignal;
import systems.intino.datamarts.subjectstore.model.signals.NumericalSignal;
import systems.intino.datamarts.subjectstore.view.history.Column;
import systems.intino.datamarts.subjectstore.view.history.Column.DoubleColumn;
import systems.intino.datamarts.subjectstore.view.history.Column.StringColumn;
import systems.intino.datamarts.subjectstore.view.history.format.ColumnDefinition;
import systems.intino.datamarts.subjectstore.view.history.format.HistoryFormat;
import systems.intino.datamarts.subjectstore.model.reducers.TextReducer;
import systems.intino.datamarts.subjectstore.model.reducers.NumberReducer;
import systems.intino.datamarts.subjectstore.model.reducers.TimeReducer;
import systems.intino.datamarts.subjectstore.view.history.format.readers.YamlHistoryFormatReader;

import java.io.*;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.*;

public class SubjectHistoryView implements Iterable<Column> {
	private final SubjectHistory history;
	private final HistoryFormat historyFormat;
	private final List<Instant> instants;
	private final Map<String, Vector<?>> vectors;

	public static Builder of(SubjectHistory subjectHistory) {
		return new Builder(subjectHistory);
	}

	public SubjectHistoryView(SubjectHistory history, HistoryFormat historyFormat) {
		this.history = history;
		this.historyFormat = historyFormat;
		this.instants = instants(historyFormat.from(), historyFormat.to(), historyFormat.duration());
		this.vectors = new HashMap<>();
		this.build();
	}

	public SubjectHistoryView crop(int top, int bottom) {
		//TODO
		return this;
	}

	public SubjectHistoryView(SubjectHistory history, String format) {
		this(history, new YamlHistoryFormatReader(format).read());
	}

	public SubjectHistoryView(SubjectHistory history, File format) throws IOException {
		this(history, new YamlHistoryFormatReader(format).read());
	}

	public Instant from() {
		return historyFormat.from();
	}

	public Instant to() {
		return historyFormat.to();
	}

	public TemporalAmount duration() {
		return historyFormat.duration();
	}

	public int size() {
		return instants.size();
	}

	public List<Instant> rows() {
		return instants;
	}

	public List<String> columns() {
		return historyFormat.columns().stream().map(c->c.name).toList();
	}

	private Column column(String name) {
		Vector<?> vector = vectors.get(name);
		if (vector instanceof DoubleVector v) return new DoubleColumn(name, v.values());
		if (vector instanceof StringVector v) return new StringColumn(name, v.values());
		return () -> name;
	}

	private Vector<?> vector(String name) {
		return vectors.get(name);
	}

	public void exportTo(OutputStream os) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
			writer.write(tsv());
		}
	}

	private void build() {
		historyFormat.columns().forEach(this::build);
	}

	private void build(ColumnDefinition columnDefinition) {
		vectors.put(columnDefinition.name, calculate(columnDefinition));
	}

	private Vector<?> calculate(ColumnDefinition columnDefinition) {
		if (columnDefinition.isAlphanumeric()) {
			return get(tagIn(columnDefinition.expression), TextReducer.of(fieldIn(columnDefinition.expression)));
		}
		else {
			return filter(calculate(columnDefinition.expression), columnDefinition.filters);
		}
	}

	private DoubleVector calculate(String definition) {
		return vectorCalculator().calculate(definition);
	}

	private Vector<?> filter(Vector<?> input, List<Filter> filters) {
		return input instanceof DoubleVector vector ?
				filter(vector.values(), filters) :
				input;
	}

	private DoubleVector filter(double[] values, List<Filter> filters) {
		for (Filter filter : filters)
			values = filter.apply(values);
		return new DoubleVector(values);
	}

	private VectorCalculator vectorCalculator() {
		return new VectorCalculator(size(), this::variable);
	}

	private DoubleVector variable(String name) {
		if (vector(name) instanceof DoubleVector vector) return vector;
		try {
			String tag = tagIn(name);
			String field = fieldIn(name);
			if (isTemporal(tag) && TimeReducer.contains(field)) return calculate(TimeReducer.of(field));
			if (NumberReducer.contains(field)) {
				return calculate(NumberReducer.of(field), history.query().number(tag).get(from(), to()));
			}
			if (TextReducer.contains(field)) return calculate(NumberReducer.of(field), history.query().text(tag).get(from(), to()));
		}
		catch (Exception ignored) {
		}
		throw new IllegalArgumentException("Variable not found: " + name);
	}

	private DoubleVector calculate(TimeReducer reducer) {
		double[] values = instants.stream().map(reducer).mapToDouble(s -> (double) s).toArray();
		return new DoubleVector(values);
	}

	private DoubleVector calculate(NumberReducer reducer, NumericalSignal signal) {
		NumericalSignal[] segments = signal.segments(duration());
		double[] values = Arrays.stream(segments).map(reducer).mapToDouble(v -> v).toArray();
		return new DoubleVector(values);
	}

	private DoubleVector calculate(NumberReducer reducer, CategoricalSignal categoricalSignal) {
		CategoricalSignal[] segments = categoricalSignal.segments(duration());
		double[] values = Arrays.stream(segments).map(reducer).mapToDouble(s -> s).toArray();
		return new DoubleVector(values);
	}

	private Vector<?> get(String attribute, TextReducer reducer) {
		CategoricalSignal categoricalSignal = history.query().text(attribute).get(from(), to());
		CategoricalSignal[] segments = categoricalSignal.segments(duration());
		String[] values = Arrays.stream(segments).map(reducer).toArray(String[]::new);
		return new StringVector(values);
	}

	private boolean isTemporal(String tag) {
		return tag.equals("ts");
	}

	private String tagIn(String name) {
		return name.split("\\.")[0];
	}

	private String fieldIn(String name) {
		return name.split("\\.")[1];
	}

	private String tsv() {
		StringBuilder sb = new StringBuilder();
		for (int row = 0; row < size(); row++) {
			StringJoiner line = new StringJoiner("\t");
			for (String column : columns())
				line.add(String.valueOf(value(row, column)));
			sb.append(line).append('\n');
		}
		return sb.toString();
	}

	private Object value(int row, String name) {
		Object o = vector(name).get(row);
		return isEmpty(o) ? "" : o;
	}

	private boolean isEmpty(Object o) {
		if (o == null) return true;
		if (o instanceof Double d) return Double.isNaN(d);
		return false;
	}

	private static List<Instant> instants(Instant from, Instant instant, TemporalAmount duration) {
		return TimeReferences
				.iterate(from, instant, duration)
				.toList();
	}

	@Override
	public String toString() {
		return "Table(" + historyFormat + ")";
	}

	@Override
	public Iterator<Column> iterator() {
		return new Iterator<>() {
			private final Iterator<String> iterator = columns().iterator();
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Column next() {
				return column(iterator.next());
			}
		};
	}

	public static class Builder {
		private final SubjectHistory history;
		private final List<ColumnDefinition> columnDefinitions;
		private Instant from;
		private Instant to;
		private TemporalAmount duration;

		public Builder(SubjectHistory history) {
			this.history = history;
			this.columnDefinitions = new ArrayList<>();
		}

		public SubjectHistoryView with(File format) throws IOException {
			return with(new YamlHistoryFormatReader(format).read());
		}

		public SubjectHistoryView with(String format)  {
			return with(new YamlHistoryFormatReader(format).read());
		}

		public SubjectHistoryView with(HistoryFormat historyFormat) {
			return new SubjectHistoryView(history, historyFormat);
		}

		public Builder from(String from) {
			return from(TimeParser.parseInstant(from));
		}

		public Builder from(Instant from) {
			this.from = from;
			return this;
		}

		public Builder to(String to) {
			return from(TimeParser.parseInstant(to));
		}

		public Builder to(Instant to) {
			this.to = to;
			return this;
		}

		public Builder duration(String duration) {
			return duration(TimeParser.parseDuration(duration));
		}

		public Builder duration(TemporalAmount duration) {
			this.duration = duration;
			return this;
		}

		public Builder add(String name, String definition, Filter... filters) {
			ColumnDefinition columnDefinition = new ColumnDefinition(name, definition);
			Arrays.stream(filters).forEach(columnDefinition::add);
			this.columnDefinitions.add(columnDefinition);
			return this;
		}

		public Builder add(List<ColumnDefinition> columnDefinitions) {
			this.columnDefinitions.addAll(columnDefinitions);
			return this;
		}

		public SubjectHistoryView build() {
			return new SubjectHistoryView(history, new HistoryFormat(from, to, duration, columnDefinitions));
		}

	}

}

