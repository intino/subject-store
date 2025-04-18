package systems.intino.datamarts.subjectstore.view.history.fields;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.time.ZoneOffset.UTC;

public interface TemporalField extends Function<Instant, Object> {
	Map<String, TemporalField> map = create();

	static Map<String, TemporalField> create() {
		Map<String, TemporalField> map = new HashMap<>();
		map.put("day-of-week", ts-> (double) zdt(ts).getDayOfWeek().getValue());
		map.put("day-of-month", ts-> (double) zdt(ts).getDayOfMonth());
		map.put("month-of-year", ts-> (double) (zdt(ts).getMonthValue()));
		map.put("quarter-of-year", ts-> (double) quarterOf(zdt(ts)));
		map.put("year", ts->  (double) zdt(ts).getYear());
		map.put("year-quarter", ts-> zdt(ts).getYear() + "Q" + quarterOf(zdt(ts)));
		map.put("year-month", ts-> zdt(ts).format(with("yyyyMM")));
		map.put("year-month-day", ts-> zdt(ts).format(with("yyyyMMdd")));
		map.put("year-month-day-hour", ts-> zdt(ts).format(with("yyyyMMddHH")));
		map.put("year-month-day-hour-minute", ts-> zdt(ts).format(with("yyyyMMddHHmm")));
		map.put("year-month-day-hour-minute-second", ts-> zdt(ts).format(with("yyyyMMddHHmmss")));
		return map;
	}


	static TemporalField of(String function) {
		return map.containsKey(function) ? map.get(function) : ts-> "Unknown function: " + function;
	}

	static boolean contains(String function) {
		return map.containsKey(function);
	}

	static ZonedDateTime zdt(Instant ts) {
		return ts.atZone(UTC);
	}

	private static Integer quarterOf(ZonedDateTime ts) {
		return (ts.getMonthValue() - 1) / 3 + 1;
	}

	private static DateTimeFormatter with(String pattern) {
		return DateTimeFormatter.ofPattern(pattern);
	}
}
