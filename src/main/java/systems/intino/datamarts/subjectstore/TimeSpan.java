package systems.intino.datamarts.subjectstore;

import java.time.Instant;

import static java.time.temporal.ChronoUnit.*;

public enum TimeSpan {
	LegacyPhase, BigBangPhase,
	ThisYear, ThisMonth, ThisWeek, Today, ThisHour, ThisMinute, ThisSecond,
	LastYearWindow, LastMonthWindow, LastWeekWindow, LastDayWindow, LastHourWindow, LasMinuteWindow, LasSecondWindow;

	public Instant from() {
		return switch (this) {
			case LegacyPhase -> TimeReference.Legacy;
			case BigBangPhase -> TimeReference.BigBang;
			case ThisYear -> TimeReference.thisYear();
			case ThisMonth -> TimeReference.thisMonth();
			case ThisWeek -> TimeReference.thisWeek();
			case Today -> TimeReference.today();
			case ThisHour -> TimeReference.thisHour();
			case ThisMinute -> TimeReference.thisMinute();
			case ThisSecond -> TimeReference.thisSecond();
			case LastYearWindow -> TimeReference.today().minus(365, DAYS);
			case LastMonthWindow -> TimeReference.today().minus(30, DAYS);
			case LastWeekWindow -> TimeReference.today().minus(7, DAYS);
			case LastDayWindow -> TimeReference.today().minus(1, DAYS);
			case LastHourWindow -> TimeReference.thisHour().minus(1, HOURS);
			case LasMinuteWindow -> TimeReference.thisMinute().minus(1, MINUTES);
			case LasSecondWindow -> TimeReference.thisSecond().minus(1, SECONDS);
		};
	}


	public Instant to() {
		return switch (this) {
			case LegacyPhase -> TimeReference.Legacy.plus(1, SECONDS);
			case BigBangPhase -> TimeReference.BigBang.plus(1, SECONDS);
			case ThisYear -> TimeReference.thisYear().plus(365, DAYS);
			case ThisMonth -> TimeReference.thisMonth().plus(30, DAYS);
			case ThisWeek -> TimeReference.thisWeek().plus(7, DAYS);
			case Today -> TimeReference.today().plus(1, DAYS);
			case ThisHour -> TimeReference.thisHour().plus(1, HOURS);
			case ThisMinute -> TimeReference.thisMinute().plus(1, MINUTES);
			case ThisSecond -> TimeReference.thisSecond().plus(1, SECONDS);
			case LastYearWindow,
				 LastMonthWindow,
				 LastWeekWindow,
				 LastDayWindow -> TimeReference.today();
			case LastHourWindow -> TimeReference.thisHour();
			case LasMinuteWindow -> TimeReference.thisMinute();
			case LasSecondWindow -> TimeReference.thisSecond();
		};
	}

}
