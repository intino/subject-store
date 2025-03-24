package io.intino.alexandria.model;

import java.time.Instant;
import java.time.ZonedDateTime;

import static java.time.DayOfWeek.MONDAY;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.*;

public class Instants {
	public static final Instant Legacy = Instant.ofEpochMilli(-6666666666666666666L);
	public static final Instant BigBang = Instant.ofEpochMilli(-1111111111111111111L);

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

	public enum TimeSpan {
		Full, LegacyPhase, BigBangPhase,
		ThisYear, ThisMonth, ThisWeek, Today, ThisHour, ThisMinute, ThisSecond,
		LastYearWindow, LastMonthWindow, LastWeekWindow, LastDayWindow, LastHourWindow, LasMinuteWindow, LasSecondWindow;

		public Instant from() {
			return switch (this) {
				case Full -> Instant.MIN;
				case LegacyPhase -> Legacy;
				case BigBangPhase -> BigBang;
				case ThisYear -> thisYear();
				case ThisMonth -> thisMonth();
				case ThisWeek -> thisWeek();
				case Today -> today();
				case ThisHour -> thisHour();
				case ThisMinute -> thisMinute();
				case ThisSecond -> thisSecond();
				case LastYearWindow -> today().minus(365, DAYS);
				case LastMonthWindow -> today().minus(30, DAYS);
				case LastWeekWindow -> today().minus(7, DAYS);
				case LastDayWindow -> today().minus(1, DAYS);
				case LastHourWindow -> thisHour().minus(1, HOURS);
				case LasMinuteWindow -> thisMinute().minus(1, MINUTES);
				case LasSecondWindow -> thisSecond().minus(1, SECONDS);
			};
		}


		public Instant to() {
			return switch (this) {
				case Full -> Instant.MAX;
				case LegacyPhase -> Legacy.plus(1, SECONDS);
				case BigBangPhase -> BigBang.plus(1, SECONDS);
				case ThisYear -> thisYear().plus(365, DAYS);
				case ThisMonth -> thisMonth().plus(30, DAYS);
				case ThisWeek -> thisWeek().plus(7, DAYS);
				case Today -> today().plus(1, DAYS);
				case ThisHour -> thisHour().plus(1, HOURS);
				case ThisMinute -> thisMinute().plus(1, MINUTES);
				case ThisSecond -> thisSecond().plus(1, SECONDS);
				case LastYearWindow,
					 LastMonthWindow,
					 LastWeekWindow,
					 LastDayWindow -> today();
				case LastHourWindow -> thisHour();
				case LasMinuteWindow -> thisMinute();
				case LasSecondWindow -> thisSecond();
			};
		}

	}
}
