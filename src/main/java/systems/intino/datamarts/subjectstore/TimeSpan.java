package systems.intino.datamarts.subjectstore;

import java.time.Instant;

import static java.time.temporal.ChronoUnit.*;

public enum TimeSpan {
	ThisYear, ThisMonth, ThisWeek, Today, ThisHour, ThisMinute, ThisSecond,
	LastYearWindow, LastMonthWindow, LastWeekWindow, LastDayWindow, LastHourWindow, LastMinuteWindow, LastSecondWindow;

	public Instant from() {
		return switch (this) {
			case ThisYear -> TimeReferences.thisYear();
			case ThisMonth -> TimeReferences.thisMonth();
			case ThisWeek -> TimeReferences.thisWeek();
			case Today -> TimeReferences.today();
			case ThisHour -> TimeReferences.thisHour();
			case ThisMinute -> TimeReferences.thisMinute();
			case ThisSecond -> TimeReferences.thisSecond();
			case LastYearWindow -> TimeReferences.today().minus(365, DAYS);
			case LastMonthWindow -> TimeReferences.today().minus(30, DAYS);
			case LastWeekWindow -> TimeReferences.today().minus(7, DAYS);
			case LastDayWindow -> TimeReferences.today().minus(1, DAYS);
			case LastHourWindow -> TimeReferences.thisHour().minus(1, HOURS);
			case LastMinuteWindow -> TimeReferences.thisMinute().minus(1, MINUTES);
			case LastSecondWindow -> TimeReferences.thisSecond().minus(1, SECONDS);
		};
	}


	public Instant to() {
		return switch (this) {
			case ThisYear -> TimeReferences.thisYear().plus(365, DAYS);
			case ThisMonth -> TimeReferences.thisMonth().plus(30, DAYS);
			case ThisWeek -> TimeReferences.thisWeek().plus(7, DAYS);
			case Today -> TimeReferences.today().plus(1, DAYS);
			case ThisHour -> TimeReferences.thisHour().plus(1, HOURS);
			case ThisMinute -> TimeReferences.thisMinute().plus(1, MINUTES);
			case ThisSecond -> TimeReferences.thisSecond().plus(1, SECONDS);
			case LastYearWindow,
				 LastMonthWindow,
				 LastWeekWindow,
				 LastDayWindow -> TimeReferences.today();
			case LastHourWindow -> TimeReferences.thisHour();
			case LastMinuteWindow -> TimeReferences.thisMinute();
			case LastSecondWindow -> TimeReferences.thisSecond();
		};
	}

}
