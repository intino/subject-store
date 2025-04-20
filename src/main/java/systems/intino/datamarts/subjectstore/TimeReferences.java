package systems.intino.datamarts.subjectstore;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.stream.Stream;

import static java.time.DayOfWeek.MONDAY;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.*;

public class TimeReferences {
	public static final Instant Legacy = Instant.ofEpochMilli(-60000000000000000L);
	public static final Instant BigBang = Instant.ofEpochMilli(-10000000000000000L);

	public static Instant thisYear() {
		return toInstant(ZonedDateTime.now(UTC).withMonth(1).withDayOfMonth(1));
	}

	public static Instant thisMonth() {
		return toInstant(ZonedDateTime.now(UTC).withDayOfMonth(1));
	}

	public static Instant thisWeek() {
		return toInstant(ZonedDateTime.now(UTC).with(MONDAY));
	}

	public static Instant thisWeek(long value) {
		return today().plus(value * 7, DAYS);
	}

	public static Instant today() {
		return Instant.now().truncatedTo(DAYS);
	}

	public static Instant today(long value) {
		return today().plus(value, DAYS);
	}

	public static Instant thisHour() {
		return Instant.now().truncatedTo(HOURS);
	}

	public static Instant thisHour(long value) {
		return thisHour().plus(value, HOURS);
	}

	public static Instant thisMinute() {
		return Instant.now().truncatedTo(MINUTES);
	}

	public static Instant thisMinute(long value) {
		return thisMinute().plus(value, MINUTES);
	}

	public static Instant thisSecond() {
		return Instant.now().truncatedTo(MINUTES);
	}

	public static Instant thisSecond(long value) {
		return thisSecond().plus(value, SECONDS);
	}

	private static Instant toInstant(ZonedDateTime dateTime) {
		return dateTime.truncatedTo(DAYS).toInstant();
	}

	public static Stream<Instant> iterate(Instant from, Instant to, TemporalAmount duration) {
		return Stream.iterate(
				from,
				instant -> instant.isBefore(to),
				instant -> add(instant, duration)
		);
	}

	public static Instant add(Instant instant, TemporalAmount duration) {
		return isFixed(duration) ?
				instant.plus(duration) :
				instant.atZone(UTC).plus(duration).toInstant();
	}

	private static boolean isFixed(TemporalAmount duration) {
		return duration.getUnits().stream()
				.noneMatch(TemporalUnit::isDateBased);
	}

}
