package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.calculator.DoubleVectorCalculator;
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
import systems.intino.datamarts.subjectstore.view.history.format.HistoryFormat.RowDefinition;
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

	private SubjectHistoryView(SubjectHistory history, HistoryFormat historyFormat) {
		this.history = history;
		this.historyFormat = historyFormat;
		this.instants = instants(historyFormat.from(), historyFormat.to(), historyFormat.period());
		this.vectors = new HashMap<>();
		this.build();
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
		return historyFormat.period();
	}

	public int size() {
		return instants.size();
	}

	public List<Instant> rows() {
		return instants;
	}

	public List<String> columns() {
		return historyFormat.columns()
				.stream()
				.map(c->c.name)
				.toList();
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

	public Exporting export() {
		return new Exporting() {
			private boolean onlyCompleteRows = false;
			private int stopOffset = 0;
			private int startOffset = 0;

			@Override
			public void to(OutputStream os) throws IOException {
				try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
					writer.write(tsv(startOffset, stopOffset, onlyCompleteRows));
				}
			}

			@Override
			public Exporting onlyCompleteRows() {
				this.onlyCompleteRows = true;
				return this;
			}

			@Override
			public Exporting start(int offset) {
				this.startOffset = offset;
				return this;
			}

			@Override
			public Exporting stop(int offset) {
				this.stopOffset = offset;
				return this;
			}
		};
	}

	public interface Exporting {
		void to(OutputStream os) throws IOException;

		Exporting onlyCompleteRows();
		Exporting start(int offset);
		Exporting stop(int offset);

	}

	private void build() {
		historyFormat.columns().forEach(this::build);
	}

	private void build(ColumnDefinition columnDefinition) {
		vectors.put(columnDefinition.name, vectorIn(columnDefinition));
	}

	private Vector<?> vectorIn(ColumnDefinition columnDefinition) {
		return isCategorical(columnDefinition) ?
				categoricalVectorIn(columnDefinition) :
				numericalVectorIn(columnDefinition);
	}

	private static final Set<String> set = Set.of("ts.year-quarter", "ts.year-month");
	private boolean isCategorical(ColumnDefinition columnDefinition) {
		return columnDefinition.type == ColumnDefinition.Type.Categorical ||
				set.contains(columnDefinition.expression);
	}

	private Vector<?> categoricalVectorIn(ColumnDefinition columnDefinition) {
		String tag = tagIn(columnDefinition.expression);
		String field = fieldIn(columnDefinition.expression);
		return tag.equals("ts") ? vectorOf(TimeReducer.of(field)) : vectorOf(tag, TextReducer.of(field));
	}

	private Vector<?> vectorOf(TimeReducer timeReducer) {
		String[] values = instants.stream().map(timeReducer)
				.map(Object::toString)
				.toArray(String[]::new);
		return new StringVector(values);
	}

	private Vector<?> numericalVectorIn(ColumnDefinition columnDefinition) {
		DoubleVector vector = vectorOf(columnDefinition.expression);
		return filter(vector, columnDefinition.filters);
	}

	private Vector<?> vectorOf(String attribute, TextReducer reducer) {
		CategoricalSignal categoricalSignal = history.query().text(attribute).get(from(), to());
		CategoricalSignal[] segments = categoricalSignal.segments(duration());
		String[] values = Arrays.stream(segments).map(reducer).toArray(String[]::new);
		return new StringVector(values);
	}

	private DoubleVector vectorOf(String definition) {
		return calculator().calculate(definition);
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

	private DoubleVectorCalculator calculator() {
		return new DoubleVectorCalculator(size(), this::variable);
	}

	private DoubleVector variable(String name) {
		if (vector(name) instanceof DoubleVector vector) return vector;
		try {
			String tag = tagIn(name);
			String field = fieldIn(name);
			if (isTemporal(tag) && TimeReducer.contains(field))
				return calculate(TimeReducer.of(field));
			if (NumberReducer.contains(field))
				return calculate(NumberReducer.of(field), history.query().number(tag).get(from(), to()));
			if (TextReducer.contains(field))
				return calculate(NumberReducer.of(field), history.query().text(tag).get(from(), to()));
		}
		catch (Exception ignored) {
		}
		throw new IllegalArgumentException("SubjectHistoryView: field not exists: " + name);
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

	private boolean isTemporal(String tag) {
		return tag.equals("ts");
	}

	private String tagIn(String name) {
		int i = name.lastIndexOf('.');
		return i > 0 ? name.substring(0, i) : name;
	}

	private String fieldIn(String name) {
		int i = name.lastIndexOf('.');
		return i > 0 ? name.substring(i+1) : "";
	}

	private String tsv(int startOffset, int stopOffset, boolean onlyCompleteRows) {
		StringBuilder sb = new StringBuilder();
		sb.append(header()).append('\n');
		for (int row = startOffset; row < size() - stopOffset; row++) {
			String line = lineOf(row) + "\n";
			if (onlyCompleteRows && isIncomplete(line)) continue;
			sb.append(line);
		}
		return sb.toString();
	}

	private boolean isIncomplete(String line) {
		return line.contains("\t\t") || line.contains("\t\n");
	}

	private String header() {
		StringJoiner joiner = new StringJoiner("\t");
		for (String column : columns()) joiner.add(column);
		return joiner.toString();
	}

	private String lineOf(int row) {
		StringJoiner joiner = new StringJoiner("\t");
		for (String column : columns())
			joiner.add(String.valueOf(value(row, column)));
		return joiner.toString();
	}

	private Object value(int row, String name) {
		Object o = vector(name).get(row);
		if (o == null) return "";
		if (o instanceof Double d && Double.isNaN(d)) return "";
		return o;
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

	@Override @SuppressWarnings("NullableProblems")
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
		private TemporalAmount period;

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

		public Builder period(String duration) {
			return period(TimeParser.parseDuration(duration));
		}

		public Builder period(TemporalAmount duration) {
			this.period = duration;
			return this;
		}

		public Builder add(String name, String definition, Filter... filters) {
			if (exists(name)) throw new IllegalArgumentException("SubjectHistoryView: column already exists: " + name);
			ColumnDefinition columnDefinition = new ColumnDefinition(name, definition);
			Arrays.stream(filters).forEach(columnDefinition::add);
			this.columnDefinitions.add(columnDefinition);
			return this;
		}

		private boolean exists(String name) {
			return columnDefinitions.stream().anyMatch(c->c.name.equals(name));
		}

		public Builder add(ColumnDefinition... columnDefinitions) {
			this.columnDefinitions.addAll(List.of(columnDefinitions));
			return this;
		}

		public Builder add(List<ColumnDefinition> columnDefinitions) {
			this.columnDefinitions.addAll(columnDefinitions);
			return this;
		}

		public Exporting export() {
			if (from == null) throw new IllegalArgumentException("SubjectHistoryView: from not set");
			if (to == null) throw new IllegalArgumentException("SubjectHistoryView: to not set");
			if (period == null) throw new IllegalArgumentException("SubjectHistoryView: period not set");
			if (columnDefinitions.isEmpty()) throw new IllegalArgumentException("SubjectHistoryView: no column is added");
			RowDefinition rowDefinition = new RowDefinition(from, to, period);
			HistoryFormat format = new HistoryFormat(rowDefinition, columnDefinitions);
			return new SubjectHistoryView(history, format).export();
		}

	}

}

