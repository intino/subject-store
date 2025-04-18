package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.calculator.VectorCalculator;
import systems.intino.datamarts.subjectstore.calculator.model.Filter;
import systems.intino.datamarts.subjectstore.calculator.model.Vector;
import systems.intino.datamarts.subjectstore.calculator.model.vectors.DoubleVector;
import systems.intino.datamarts.subjectstore.calculator.model.vectors.ObjectVector;
import systems.intino.datamarts.subjectstore.model.signals.CategoricalSignal;
import systems.intino.datamarts.subjectstore.model.signals.NumericalSignal;
import systems.intino.datamarts.subjectstore.history.view.Column;
import systems.intino.datamarts.subjectstore.history.view.Format;
import systems.intino.datamarts.subjectstore.history.view.fields.CategoricalField;
import systems.intino.datamarts.subjectstore.history.view.fields.NumericalField;
import systems.intino.datamarts.subjectstore.history.view.fields.TemporalField;
import systems.intino.datamarts.subjectstore.history.view.format.YamlFileFormatReader;
import systems.intino.datamarts.subjectstore.history.view.format.YamlFormatReader;

import java.io.*;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.*;

public class SubjectHistoryView {
	private final SubjectHistory store;
	private final Format format;
	private final List<Instant> instants;
	private final Map<String, Vector<?>> vectors;

	public SubjectHistoryView(SubjectHistory store, Format format) {
		this.store = store;
		this.format = format;
		this.instants = instants(format.from(), format.to(), format.duration());
		this.vectors = new HashMap<>();
		this.build();
	}

	public SubjectHistoryView(SubjectHistory store, String format) {
		this(store, new YamlFormatReader(format).read());
	}

	public SubjectHistoryView(SubjectHistory store, File format) throws IOException {
		this(store, new YamlFileFormatReader(format).read());
	}

	public List<Column> columns() {
		return format.columns();
	}

	private String column(int i) {
		return format.columns().get(i).name;
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

	public int rows() {
		return instants.size();
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
		columns().forEach(this::build);
	}

	private void build(Column column) {
		vectors.put(column.name, calculate(column));
	}

	private Vector<?> calculate(Column column) {
		if (column.isAlphanumeric()) {
			return get(tagIn(column.definition), CategoricalField.of(fieldIn(column.definition)));
		}
		else {
			return filter(calculate(column.definition), column.filters);
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
		return new VectorCalculator(rows(), this::variable);
	}

	private DoubleVector variable(String name) {
		if (vectors.get(name) instanceof DoubleVector vector) return vector;
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
		NumericalSignal signal = store.query().number(tag).get(from(), to());
		NumericalSignal[] segments = signal.segments(duration());
		double[] values = Arrays.stream(segments).map(function).mapToDouble(v -> v).toArray();
		return new DoubleVector(values);
	}

	private DoubleVector calculate(String tag, CategoricalField function) {
		CategoricalSignal categoricalSignal = store.query().text(tag).get(from(), to());
		CategoricalSignal[] segments = categoricalSignal.segments(duration());
		double[] values = Arrays.stream(segments).map(function).mapToDouble(s -> (double) s).toArray();
		return new DoubleVector(values);
	}

	private Vector<?> get(String attribute, CategoricalField function) {
		CategoricalSignal categoricalSignal = store.query().text(attribute).get(from(), to());
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
		for (int i = 0; i < rows(); i++) {
			StringJoiner row = new StringJoiner("\t");
			for (int j = 0; j < columns().size(); j++)
				row.add(String.valueOf(value(i, j)));
			sb.append(row).append('\n');
		}
		return sb.toString();
	}

	private Object value(int i, int j) {
		Object o = vectors.get(column(j)).get(i);
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



}
