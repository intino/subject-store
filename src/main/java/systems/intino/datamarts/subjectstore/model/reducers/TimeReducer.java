package systems.intino.datamarts.subjectstore.model.reducers;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.time.ZoneOffset.UTC;

public interface TimeReducer extends Function<Instant, Object> {
	Map<String, TimeReducer> map = create();

	static Map<String, TimeReducer> create() {
		Map<String, TimeReducer> map = new HashMap<>();
		map.put("hour-of-day", ts-> (double) zdt(ts).getHour());
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


	static TimeReducer of(String field) {
		return map.getOrDefault(field, ts-> "Unknown field: " + field);
	}

	static boolean contains(String field) {
		return map.containsKey(field);
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
