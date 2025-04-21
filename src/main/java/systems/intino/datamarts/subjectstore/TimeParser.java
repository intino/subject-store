package systems.intino.datamarts.subjectstore;

import java.time.*;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class TimeParser {

	private static final List<Function<String, Instant>> ParseInstants = List.of(
		TimeParser::year,
		TimeParser::yearMonth,
		TimeParser::localDate,
		TimeParser::instant

	);

	public static Instant parseInstant(String input) {
		return ParseInstants.stream()
				.map(f->f.apply(input))
				.filter(Objects::nonNull)
				.findFirst()
				.orElse(null);
	}

	private static Instant year(String input) {
		try {
			Year year = Year.parse(input);
			return year.atMonth(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();
		} catch (DateTimeParseException e) {
			return null;
		}
	}

	private static Instant yearMonth(String input) {
		try {
			YearMonth ym = YearMonth.parse(input);
			return ym.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();
		} catch (DateTimeParseException e) {
			return null;
		}
	}

	private static Instant localDate(String input) {
		try {
			LocalDate date = LocalDate.parse(input);
			return date.atStartOfDay(ZoneOffset.UTC).toInstant();
		} catch (DateTimeParseException e) {
			return null;
		}
	}

	private static Instant instant(String input) {
		try {
			return Instant.parse(input);
		} catch (DateTimeParseException e) {
			return null;
		}
	}

	public static TemporalAmount parseDuration(String duration) {
		return duration.contains("T") ? Duration.parse(duration) : Period.parse(duration);
	}
}
