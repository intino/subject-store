package tests.history;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.history.model.Point;
import systems.intino.datamarts.subjectstore.history.model.Signal;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.List;
import java.util.stream.IntStream;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;
import static systems.intino.datamarts.subjectstore.TimeReference.today;

@SuppressWarnings("NewClassNamingConvention")
public class Signal_ {

	@Test
	public void should_create_empty_signal_with_correct_bounds_and_summary() {
		Signal.Raw signal = new Signal.Raw(today(-15), today(15), List.of());
		assertThat(signal.from()).isEqualTo(today(-15));
		assertThat(signal.to()).isEqualTo(today(15));
		assertThat(signal.duration()).isEqualTo(Duration.of(30, DAYS));
		assertThat(signal.isEmpty()).isTrue();
		assertThat(signal.count()).isEqualTo(0);
		assertThat(signal.summary().count()).isEqualTo(0);
		assertThat(signal.summary().sum()).isEqualTo(0);
		assertThat(signal.summary().mean()).isEqualTo(0);
		assertThat(signal.summary().sd()).isNaN();
		assertThat(signal.summary().min()).isNull();
		assertThat(signal.summary().max()).isNull();
		assertThat(signal.summary().range()).isNaN();
	}

	@Test
	public void should_return_identical_summary_for_signals_with_same_points() {
		Signal.Raw signal1 = new Signal.Raw(today(-15), today(15), points(-5, 5));
		Signal.Raw signal2 = new Signal.Raw(today(-10), today(10), points(-5,5));
		assertThat(signal1.summary()).isEqualTo(signal2.summary());
	}

	@Test
	public void should_calculate_summary() {
		Signal.Raw signal = new Signal.Raw(today(0), today(35), points(0, 30));
		assertThat(signal.count()).isEqualTo(24*30);
		assertThat(signal.from()).isEqualTo(today(0));
		assertThat(signal.to()).isEqualTo(today(35));
		assertThat(signal.duration()).isEqualTo(Duration.of(35,DAYS));
		assertThat(signal.summary().count()).isEqualTo(24*30);
		assertThat(signal.summary().sum()).isCloseTo(1955, withPercentage(0.1));
		assertThat(signal.summary().mean()).isCloseTo(2.715, withPercentage(0.1));
		assertThat(signal.summary().sd()).isCloseTo(707.303, withPercentage(0.1));
		assertThat(signal.summary().min().value()).isCloseTo(-1000, withPercentage(0.1));
		assertThat(signal.summary().max().value()).isCloseTo(1000, withPercentage(0.1));
		assertThat(signal.summary().range()).isCloseTo(2000, withPercentage(0.1));
		assertThat(signal.distribution().min()).isCloseTo(-1000, withPercentage(0.1));
		assertThat(signal.distribution().q1()).isCloseTo(-705, withPercentage(1));
		assertThat(signal.distribution().q2()).isCloseTo(6, withPercentage(20));
		assertThat(signal.distribution().median()).isCloseTo(6, withPercentage(20));
		assertThat(signal.distribution().q3()).isCloseTo(706, withPercentage(1));
		assertThat(signal.distribution().max()).isCloseTo(1000, withPercentage(0.1));
		assertThat(signal.distribution().probabilityLeftTail(706)).isCloseTo(0.75, withPercentage(1));
		assertThat(signal.distribution().probabilityRightTail(706)).isCloseTo(0.25, withPercentage(1));
		assertThat(signal.distribution().probabilityLeftTail(6)).isCloseTo(0.5, withPercentage(1));
		assertThat(signal.distribution().probabilityRightTail(6)).isCloseTo(0.5, withPercentage(1));
		assertThat(signal.distribution().probabilityLeftTail(-705)).isCloseTo(0.25, withPercentage(1));
		assertThat(signal.distribution().probabilityRightTail(-705)).isCloseTo(0.75, withPercentage(1));
	}

	@Test
	public void should_segment_into_daily_segments() {
		Signal.Raw signal = new Signal.Raw(today(-1), today(5), points(0, 4));
		Signal[] segments = signal.segments(Duration.ofDays(1));
		assertThat(segments.length).isEqualTo(6);
		assertThat(signal.segments(6)).isEqualTo(segments);
		assertThat(segments[0].count()).isEqualTo(0);
		assertThat(segments[0].from()).isEqualTo(today(-1));
		assertThat(segments[0].to()).isEqualTo(today(0));
		assertThat(segments[0].duration()).isEqualTo(Duration.ofDays(1));
		assertThat(segments[0].summary().max()).isNull();
		assertThat(segments[0].summary().min()).isNull();
		assertThat(segments[0].summary().sum()).isEqualTo(0);
		assertThat(segments[0].summary().mean()).isEqualTo(0);
		assertThat(segments[0].summary().sd()).isNaN();
		assertThat(segments[1].count()).isEqualTo(24);
		assertThat(segments[1].from()).isEqualTo(today(0));
		assertThat(segments[1].to()).isEqualTo(today(1));
		assertThat(segments[1].duration()).isEqualTo(Duration.ofDays(1));
		assertThat(segments[1].summary().min().value()).isCloseTo(-999, withPercentage(0.1));
		assertThat(segments[1].summary().max().value()).isCloseTo(990, withPercentage(0.1));
		assertThat(segments[1].summary().sum()).isCloseTo(979, withPercentage(0.1));
		assertThat(segments[1].summary().mean()).isCloseTo(40.825, withPercentage(0.1));
		assertThat(segments[1].summary().sd()).isCloseTo(712.414, withPercentage(0.1));
		assertThat(segments[1].distribution().q1()).isCloseTo(-544, withPercentage(0.1));
		assertThat(segments[1].distribution().q2()).isCloseTo(141., withPercentage(0.1));
		assertThat(segments[1].distribution().q3()).isCloseTo(836, withPercentage(0.1));
		assertThat(segments[1].segments(4)[0].duration()).isEqualTo(Duration.of(6, HOURS));
		assertThat(segments[1].segments(4)[0].count()).isEqualTo(6);
		assertThat(segments[1].segments(4)[1].duration()).isEqualTo(Duration.of(6, HOURS));
		assertThat(segments[1].segments(4)[1].count()).isEqualTo(6);
		assertThat(segments[1].segments(4)[2].duration()).isEqualTo(Duration.of(6, HOURS));
		assertThat(segments[1].segments(4)[2].count()).isEqualTo(6);
		assertThat(segments[1].segments(4)[3].duration()).isEqualTo(Duration.of(6, HOURS));
		assertThat(segments[1].segments(4)[3].count()).isEqualTo(6);
		assertThat(segments[2].count()).isEqualTo(24);
		assertThat(segments[3].count()).isEqualTo(24);
		assertThat(segments[4].count()).isEqualTo(24);
		assertThat(segments[5].count()).isEqualTo(0);
		assertThat(segments[5].summary().count()).isEqualTo(0);
		assertThat(segments[5].distribution().count()).isEqualTo(0);
		assertThat(segments[5].isEmpty()).isTrue();
	}

	@Test
	public void should_segment_signal_into_monthly_segments() {
		Instant from = Instant.parse("2025-01-01T00:00:00Z");
		Signal signal = new Signal.Raw(from, from.plus(365, DAYS), List.of());
		Signal[] segments = signal.segments(Period.ofMonths(1));
		assertThat(segments.length).isEqualTo(12);
		assertThat(segments[0].duration()).isEqualTo(Duration.ofDays(31));
		assertThat(segments[1].duration()).isEqualTo(Duration.ofDays(28));
		assertThat(segments[2].duration()).isEqualTo(Duration.ofDays(31));
		assertThat(segments[3].duration()).isEqualTo(Duration.ofDays(30));
	}

	private List<Point<Double>> points(int from, int to) {
		return IntStream.range(from * 24, to * 24)
				.mapToObj(Signal_::point)
				.collect(toList());
	}

	private static Point<Double> point(int i) {
		return new Point<>(feed(i), hour(i), value(i));
	}

	private static Instant hour(int i) {
		return today().plus(i, HOURS);
	}

	private static int feed(int i) {
		return i / 20;
	}

	private static double value(int value) {
		return Math.sin(value) * 1000;
	}

}
