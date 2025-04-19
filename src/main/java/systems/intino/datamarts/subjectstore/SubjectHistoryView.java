package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.calculator.VectorCalculator;
import systems.intino.datamarts.subjectstore.calculator.model.Filter;
import systems.intino.datamarts.subjectstore.calculator.model.Vector;
import systems.intino.datamarts.subjectstore.calculator.model.vectors.DoubleVector;
import systems.intino.datamarts.subjectstore.calculator.model.vectors.ObjectVector;
import systems.intino.datamarts.subjectstore.model.signals.CategoricalSignal;
import systems.intino.datamarts.subjectstore.model.signals.NumericalSignal;
import systems.intino.datamarts.subjectstore.view.history.ColumnDefinition;
import systems.intino.datamarts.subjectstore.view.history.Format;
import systems.intino.datamarts.subjectstore.view.history.fields.CategoricalField;
import systems.intino.datamarts.subjectstore.view.history.fields.NumericalField;
import systems.intino.datamarts.subjectstore.view.history.fields.TemporalField;
import systems.intino.datamarts.subjectstore.view.history.format.YamlFileFormatReader;
import systems.intino.datamarts.subjectstore.view.history.format.YamlFormatReader;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.*;

public class SubjectHistoryView {
	private final SubjectHistory history;
	private final Format format;
	private final List<Instant> instants;
	private final Map<String, Vector<?>> columns;

	public static Builder of(SubjectHistory subjectHistory) {
		return new Builder(subjectHistory);
	}

	public SubjectHistoryView(SubjectHistory history, Format format) {
		this.history = history;
		this.format = format;
		this.instants = instants(format.from(), format.to(), format.duration());
		this.columns = new HashMap<>();
		this.build();
	}

	public SubjectHistoryView(SubjectHistory history, String format) {
		this(history, new YamlFormatReader(format).read());
	}

	public SubjectHistoryView(SubjectHistory history, File format) throws IOException {
		this(history, new YamlFileFormatReader(format).read());
	}

	public Instant from() {
		return format.from();
	}

	public Instant to() {
		return format.to();
	}

	public TemporalAmount duration() {
		return format.duration();
	}

	public int size() {
		return instants.size();
	}

	public List<Instant> rows() {
		return instants;
	}

	public List<String> columns() {
		return format.columns().stream().map(c->c.name).toList();
	}

	private Vector<?> column(String name) {
		return columns.get(name);
	}

	public void exportTo(File file) throws IOException {
		try (OutputStream os = new FileOutputStream(file)) {
			exportTo(os);
		}
	}

	public void exportTo(OutputStream os) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
			writer.write(tsv());
		}
	}

	private void build() {
		format.columns().forEach(this::build);
	}

	private void build(ColumnDefinition columnDefinition) {
		columns.put(columnDefinition.name, calculate(columnDefinition));
	}

	private Vector<?> calculate(ColumnDefinition columnDefinition) {
		if (columnDefinition.isAlphanumeric()) {
			return get(tagIn(columnDefinition.definition), CategoricalField.of(fieldIn(columnDefinition.definition)));
		}
		else {
			return filter(calculate(columnDefinition.definition), columnDefinition.filters);
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
		if (column(name) instanceof DoubleVector vector) return vector;
		try {
			String tag = tagIn(name);
			String field = fieldIn(name);
			if (isTemporal(tag) && TemporalField.contains(field)) return calculate(TemporalField.of(field));
			if (CategoricalField.contains(field)) return calculate(tag, CategoricalField.of(field));
			if (NumericalField.contains(field)) return calculate(tag, NumericalField.of(field));
		}
		catch (Exception ignored) {
		}
		throw new IllegalArgumentException("Variable not found: " + name);
	}

	private DoubleVector calculate(TemporalField temporalField) {
		double[] values = instants.stream().map(temporalField).mapToDouble(s -> (double) s).toArray();
		return new DoubleVector(values);
	}

	private DoubleVector calculate(String tag, NumericalField function) {
		NumericalSignal signal = history.query().number(tag).get(from(), to());
		NumericalSignal[] segments = signal.segments(duration());
		double[] values = Arrays.stream(segments).map(function).mapToDouble(v -> v).toArray();
		return new DoubleVector(values);
	}

	private DoubleVector calculate(String tag, CategoricalField function) {
		CategoricalSignal categoricalSignal = history.query().text(tag).get(from(), to());
		CategoricalSignal[] segments = categoricalSignal.segments(duration());
		double[] values = Arrays.stream(segments).map(function).mapToDouble(s -> (double) s).toArray();
		return new DoubleVector(values);
	}

	private Vector<?> get(String attribute, CategoricalField function) {
		CategoricalSignal categoricalSignal = history.query().text(attribute).get(from(), to());
		CategoricalSignal[] segments = categoricalSignal.segments(duration());
		Object[] values = Arrays.stream(segments).map(function).toArray(Object[]::new);
		return new ObjectVector(values);
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
		Object o = column(name).get(row);
		return isEmpty(o) ? "" : o;
	}

	private boolean isEmpty(Object o) {
		if (o == null) return true;
		if (o instanceof Double d) return Double.isNaN(d);
		return false;
	}

	private static List<Instant> instants(Instant from, Instant instant, TemporalAmount duration) {
		return TimeReference
				.iterate(from, instant, duration)
				.toList();
	}

	@Override
	public String toString() {
		return "Table(" + format + ")";
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
			return with(new YamlFileFormatReader(format).read());
		}

		public SubjectHistoryView with(Format format) {
			return new SubjectHistoryView(history, format);
		}

		public Builder from(Instant from) {
			this.from = from;
			return this;
		}

		public Builder to(Instant to) {
			this.to = to;
			return this;
		}

		public Builder duration(Duration duration) {
			this.duration = duration;
			return this;
		}

		public Builder period(Period period) {
			this.duration = period;
			return this;
		}

		public Builder add(ColumnDefinition columnDefinition) {
			this.columnDefinitions.add(columnDefinition);
			return this;
		}

		public Builder add(List<ColumnDefinition> columnDefinitions) {
			this.columnDefinitions.addAll(columnDefinitions);
			return this;
		}

		public SubjectHistoryView build() {
			return new SubjectHistoryView(history, new Format(from, to, duration, columnDefinitions));
		}

	}

}

