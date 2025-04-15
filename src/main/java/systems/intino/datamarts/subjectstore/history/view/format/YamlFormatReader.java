package systems.intino.datamarts.subjectstore.history.view.format;

import org.yaml.snakeyaml.Yaml;
import systems.intino.datamarts.subjectstore.calculator.model.filters.*;
import systems.intino.datamarts.subjectstore.calculator.model.Filter;
import systems.intino.datamarts.subjectstore.history.view.Column;
import systems.intino.datamarts.subjectstore.history.view.Format;
import systems.intino.datamarts.subjectstore.history.view.FormatReader;

import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.stream.Stream;

import static systems.intino.datamarts.subjectstore.history.view.format.TemporalParser.parseInstant;
import static systems.intino.datamarts.subjectstore.history.view.format.TemporalParser.parsePeriod;


public class YamlFormatReader implements FormatReader {
	private final String content;

	public YamlFormatReader(String content) {
		this.content = content;
	}

	@Override
	public Format read() {
		FormatDefinition formatDefinition = new Yaml().loadAs(content, FormatDefinition.class);
		return formatOf(formatDefinition);
	}

	private Format formatOf(FormatDefinition definition) {
		Instant from = parseInstant(definition.rows.from);
		Instant to = parseInstant(definition.rows.to);
		TemporalAmount period = parsePeriod(definition.rows.period);
		return new Format(from, to, period)
				.add(columnsIn(definition.columns));
	}


	private List<Column> columnsIn(List<ColumnDefinition> definitions) {
		return stream(definitions)
				.map(this::column)
				.toList();
	}

	private Column column(ColumnDefinition definition) {
		return new Column(definition.name, definition.calc).
				add(filtersIn(definition.filters));
	}

	private List<Filter> filtersIn(List<String> definitions) {
		return stream(definitions)
				.map(this::filter)
				.toList();
	}

	private Filter filter(String definition) {
		return filter(new FilterDefinition(definition));
	}

	private Filter filter(FilterDefinition definition) {
		return switch (definition.type()) {
			case "Sin" -> new SinFilter();
			case "Cos" -> new CosFilter();
			case "MinMaxNormalization" -> new MinMaxNormalizationFilter();
			case "ZScoreNormalization" -> new ZScoreNormalizationFilter();
			case "CumulativeSum" -> new CumulativeSumFilter();
			case "Differencing" -> new DifferencingFilter();
			case "Lag" -> new LagFilter(definition.asInteger(1));
			case "RollingAverage" -> new RollingAverageFilter(definition.asInteger(1));
			case "RollingSum" -> new RollingSumFilter(definition.asInteger(1));
			case "RollingMax" -> new RollingMaxFilter(definition.asInteger(1));
			case "RollingMin" -> new RollingMinFilter(definition.asInteger(1));
			case "RollingStandardDeviation" -> new RollingStandardDeviationFilter(definition.asInteger(1));
			case "BinaryThreshold" -> new BinaryThresholdFilter(definition.asDouble(1));
			default -> throw new IllegalArgumentException("Unknown filter type: " + definition);
		};	}

	private <T> Stream<T>  stream(List<T> definitions) {
		return definitions != null ? definitions.stream() : Stream.empty();
	}


	private static class FormatDefinition {
		public Rows rows;
		public List<ColumnDefinition> columns;
	}

	private static class Rows {
		public String from;
		public String to;
		public String period;
	}

	private static class ColumnDefinition {
		public String name;
		public String calc;
		public List<String> filters;
	}

	private record FilterDefinition(String value) {

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
