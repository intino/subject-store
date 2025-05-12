package systems.intino.datamarts.subjectstore.view.history.format.readers;

import org.yaml.snakeyaml.Yaml;
import systems.intino.datamarts.subjectstore.calculator.model.filters.*;
import systems.intino.datamarts.subjectstore.calculator.model.Filter;
import systems.intino.datamarts.subjectstore.view.history.format.ColumnDefinition;
import systems.intino.datamarts.subjectstore.view.history.format.HistoryFormat;
import systems.intino.datamarts.subjectstore.view.history.format.HistoryFormatReader;

import java.io.*;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.stream.Stream;

import static systems.intino.datamarts.subjectstore.TimeParser.parseInstant;
import static systems.intino.datamarts.subjectstore.TimeParser.parseDuration;


public class YamlHistoryFormatReader implements HistoryFormatReader {
	private final String format;

	public YamlHistoryFormatReader(String format) {
		this.format = format;
	}

	public YamlHistoryFormatReader(File format) throws IOException {
		this(read(format));
	}

	private static String read(File file) throws IOException {
		try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
			return new String(is.readAllBytes());
		}
	}


	@Override
	public HistoryFormat read() {
		PojoFormat pojoFormat = new Yaml().loadAs(format, PojoFormat.class);
		return map(pojoFormat);
	}

	private HistoryFormat map(PojoFormat format) {
		Instant from = parseInstant(format.rows.from);
		Instant to = parseInstant(format.rows.to);
		TemporalAmount period = parseDuration(format.rows.period);
		return new HistoryFormat(from, to, period)
				.add(map(format.columns));
	}


	private List<ColumnDefinition> map(List<PojoColumn> columns) {
		return stream(columns)
				.map(this::map)
				.toList();
	}

	private ColumnDefinition map(PojoColumn column) {
		return new ColumnDefinition(column.name, column.calc).
				add(filtersIn(column.filters));
	}

	private List<Filter> filtersIn(List<String> definitions) {
		return stream(definitions)
				.map(this::map)
				.toList();
	}

	private Filter map(String filter) {
		return map(new PojoFilter(filter));
	}

	private Filter map(PojoFilter filter) {
		return switch (filter.type()) {
			case "Sin" -> new SinFilter();
			case "Cos" -> new CosFilter();
			case "MinMaxNormalization" -> new MinMaxNormalizationFilter();
			case "ZScoreNormalization" -> new ZScoreNormalizationFilter();
			case "CumulativeSum" -> new CumulativeSumFilter();
			case "Differencing" -> new DifferencingFilter();
			case "Lag" -> new LagFilter(filter.asInteger(1));
			case "RollingAverage" -> new RollingAverageFilter(filter.asInteger(1));
			case "RollingSum" -> new RollingSumFilter(filter.asInteger(1));
			case "RollingMax" -> new RollingMaxFilter(filter.asInteger(1));
			case "RollingMin" -> new RollingMinFilter(filter.asInteger(1));
			case "RollingStandardDeviation" -> new RollingStandardDeviationFilter(filter.asInteger(1));
			case "BinaryThreshold" -> new BinaryThresholdFilter(filter.asDouble(1));
			default -> throw new IllegalArgumentException("Unknown filter type: " + filter);
		};	}

	private <T> Stream<T>  stream(List<T> definitions) {
		return definitions != null ? definitions.stream() : Stream.empty();
	}


	private static class PojoFormat {
		public PojoRows rows;
		public List<PojoColumn> columns;
	}

	private static class PojoRows {
		public String from;
		public String to;
		public String period;
	}

	private static class PojoColumn {
		public String name;
		public String calc;
		public List<String> filters;
	}

	private record PojoFilter(String value) {

		public String type() {
			return value.split(":")[0];
		}

		public int asInteger(int index) {
			try {
				return Integer.parseInt(value.split(":")[index]);
			}
			catch (Exception e) {
				throw new IllegalArgumentException("Invalid column definition: " + value + " > Integer value required at index " + index);
			}
		}

		public double asDouble(int index) {
			try {
				return Double.parseDouble(value.split(":")[index]);
			}
			catch (Exception e) {
				throw new IllegalArgumentException("Invalid column definition: " + value + " > Double value required at index " + index);
			}
		}
	}

}
