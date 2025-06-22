package systems.intino.datamarts.subjectstore.model.reducers;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static java.time.ZoneOffset.UTC;

public interface TimeReducer extends Function<Instant, Object> {
	Map<String, TimeReducer> map = create();

	static Map<String, TimeReducer> create() {
		Map<String, TimeReducer> map = new HashMap<>();
		map.put("year", ts->  (double) zdt(ts).getYear());
		map.put("quarter-of-year", ts-> (double) quarterOf(zdt(ts)));
		map.put("month-of-year", ts-> (double) (zdt(ts).getMonthValue()));
		map.put("day-of-month", ts-> (double) zdt(ts).getDayOfMonth());
		map.put("week-of-year", ts-> (double) weekOf(zdt(ts)));
		map.put("day-of-week", ts-> (double) zdt(ts).getDayOfWeek().getValue());
		map.put("hour-of-day", ts-> (double) zdt(ts).getHour());
		map.put("minute-of-hour", ts-> (double) zdt(ts).getMinute());
		map.put("second-of-minute", ts-> (double) zdt(ts).getSecond());
		map.put("year-quarter", ts-> zdt(ts).getYear() + "Q" + quarterOf(zdt(ts)));
		map.put("year-month", ts-> zdt(ts).format(with("yyyyMM")));
		map.put("year-month-day", ts-> zdt(ts).format(with("yyyyMMdd")));
		map.put("year-month-day-hour", ts-> zdt(ts).format(with("yyyyMMddHH")));
		map.put("year-month-day-hour-minute", ts-> zdt(ts).format(with("yyyyMMddHHmm")));
		map.put("year-month-day-hour-minute-second", ts-> zdt(ts).format(with("yyyyMMddHHmmss")));
		map.put("epoch-years", TimeReducer::epochYears);
		map.put("epoch-months", TimeReducer::epochMonths);
		map.put("epoch-days", TimeReducer::epochDays);
		map.put("epoch-hours", TimeReducer::epochHours);
		map.put("epoch-minutes", TimeReducer::epochMinutes);
		map.put("epoch-seconds", TimeReducer::epochSeconds);
		map.put("iso", Instant::toString);
		return map;
	}

	static int epochYears(Instant ts) {
		return zdt(ts).getYear() - 1970;
	}

	static int epochMonths(Instant ts) {
		ZonedDateTime zdt = zdt(ts);
		return (zdt.getYear() - 1970) * 12 + zdt.getMonthValue();
	}

	static int epochDays(Instant ts) {
		return epochHours(ts) / 24;
	}

	static int epochHours(Instant ts) {
		return epochMinutes(ts) / 60;
	}

	static int epochMinutes(Instant ts) {
		return epochSeconds(ts) / 60;
	}

	static int epochSeconds(Instant ts) {
		return (int) ts.getEpochSecond();
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

	static int weekOf(ZonedDateTime zdt) {
		WeekFields weekFields = WeekFields.of(Locale.getDefault());
		return zdt.get(weekFields.weekOfYear());
	}

	private static DateTimeFormatter with(String pattern) {
		return DateTimeFormatter.ofPattern(pattern);
	}
}
