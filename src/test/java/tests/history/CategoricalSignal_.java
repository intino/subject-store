package tests.history;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.model.signals.CategoricalSignal;
import systems.intino.datamarts.subjectstore.model.Signal;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.IntStream;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;
import static systems.intino.datamarts.subjectstore.TimeReferences.today;

@SuppressWarnings("NewClassNamingConvention")
public class CategoricalSignal_ {
	@Test
	public void should_create_empty_sequence_with_correct_bounds_and_summary() {
		CategoricalSignal.Raw sequence = new CategoricalSignal.Raw(today(-15), today(15), List.of());
		assertThat(sequence.from()).isEqualTo(today(-15));
		assertThat(sequence.to()).isEqualTo(today(15));
		assertThat(sequence.duration()).isEqualTo(Duration.of(30, DAYS));
		assertThat(sequence.isEmpty()).isTrue();
		assertThat(sequence.count()).isEqualTo(0);
		assertThat(sequence.summary().count()).isEqualTo(0);
		assertThat(sequence.summary().entropy()).isEqualTo(0);
	}

	@Test
	public void should_calculate_summary() {
		CategoricalSignal.Raw sequence = new CategoricalSignal.Raw(today(0), today(35), points(0, 30));
		assertThat(sequence.count()).isEqualTo(24*30);
		assertThat(sequence.from()).isEqualTo(today(0));
		assertThat(sequence.to()).isEqualTo(today(35));
		assertThat(sequence.duration()).isEqualTo(Duration.of(35,DAYS));
		assertThat(sequence.summary().count()).isEqualTo(24*30);
		assertThat(sequence.summary().categories().size()).isEqualTo(15);
		assertThat(sequence.summary().mode()).isEqualTo("lorem");
		assertThat(sequence.summary().frequency("lorem")).isEqualTo(127);
		assertThat(sequence.summary().frequency("ipsum")).isEqualTo(43);
	}

	@Test
	public void should_calculate_highest_and_lowest() {
		CategoricalSignal.Raw sequence = new CategoricalSignal.Raw(today(0), today(35), List.of(
				new Signal.Point<>(0, Instant.now(), "A"),
				new Signal.Point<>(1, Instant.now(), "B"),
				new Signal.Point<>(2, Instant.now(), "B"),
				new Signal.Point<>(3, Instant.now(), "A"),
				new Signal.Point<>(4, Instant.now(), "A"),
				new Signal.Point<>(5, Instant.now(), "C"),
				new Signal.Point<>(6, Instant.now(), "A")
		));

		assertThat(sequence.count()).isEqualTo(7);

		assertThat(sequence.summary().frequency("A")).isEqualTo(4);
		assertThat(sequence.summary().frequency("B")).isEqualTo(2);
		assertThat(sequence.summary().frequency("C")).isEqualTo(1);
		assertThat(sequence.summary().frequency("D")).isEqualTo(0);

		assertThat(sequence.summary().highest().getKey()).isEqualTo("A");
		assertThat(sequence.summary().highest().getValue()).isEqualTo(4);

		assertThat(sequence.summary().lowest().getKey()).isEqualTo("C");
		assertThat(sequence.summary().lowest().getValue()).isEqualTo(1);
	}

	@Test
	public void should_return_identical_summary_for_sequences_with_same_points() {
		CategoricalSignal.Raw signal1 = new CategoricalSignal.Raw(today(-15), today(15), points(-5, 5));
		CategoricalSignal.Raw signal2 = new CategoricalSignal.Raw(today(-10), today(10), points(-5,5));
		assertThat(signal1.summary()).isEqualTo(signal2.summary());
	}

	@Test
	public void should_segment_sequence_into_daily_segments() {
		CategoricalSignal.Raw sequence = new CategoricalSignal.Raw(today(-1), today(5), points(0, 4));
		CategoricalSignal[] segments = sequence.segments(Duration.ofDays(1));
		assertThat(segments.length).isEqualTo(6);
		assertThat(sequence.segments(6)).isEqualTo(segments);
		assertThat(segments[0].count()).isEqualTo(0);
		assertThat(segments[0].from()).isEqualTo(today(-1));
		assertThat(segments[0].to()).isEqualTo(today(0));
		assertThat(segments[0].duration()).isEqualTo(Duration.ofDays(1));
		assertThat(segments[0].summary().mode()).isNull();
		assertThat(segments[0].summary().categories().size()).isEqualTo(0);
		assertThat(segments[1].count()).isEqualTo(24);
		assertThat(segments[1].from()).isEqualTo(today(0));
		assertThat(segments[1].to()).isEqualTo(today(1));
		assertThat(segments[1].duration()).isEqualTo(Duration.ofDays(1));
		assertThat(segments[1].summary().mode()).isEqualTo("lorem");
		assertThat(segments[1].summary().categories().size()).isEqualTo(15);
		assertThat(segments[1].summary().frequency("lorem")).isEqualTo(4);
		assertThat(segments[1].summary().entropy()).isCloseTo(2.6004, withinPercentage(0.01));
		assertThat(segments[5].summary().count()).isEqualTo(0);
		assertThat(segments[5].summary().categories()).isEmpty();
		assertThat(segments[5].summary().mode()).isNull();
		assertThat(segments[5].summary().entropy()).isEqualTo(0);
	}

	@Test
	public void should_segment_sequence_into_yearly_segments() {
		CategoricalSignal.Raw sequence = new CategoricalSignal.Raw(today(0), today(3650), List.of());
		CategoricalSignal[] segments = sequence.segments(Period.ofYears(1));
		assertThat(segments.length).isEqualTo(10);
		assertThat(segments[0].duration()).isEqualTo(Duration.ofDays(365));
		assertThat(segments[1].duration()).isEqualTo(Duration.ofDays(365));
		assertThat(segments[2].duration()).isEqualTo(Duration.ofDays(366));
		assertThat(segments[3].duration()).isEqualTo(Duration.ofDays(365));

	}


	@SuppressWarnings("SameParameterValue")
	private List<Signal.Point<String>> points(int from, int to) {
		return IntStream.range(from * 24, to * 24)
				.mapToObj(this::point)
				.collect(toList());
	}

	private Signal.Point<String> point(int i) {
		return new Signal.Point<>(feed(i), hour(i), value(i));
	}

	private static int feed(int i) {
		return i / 20;
	}

	private static final Instant now = Instant.now().truncatedTo(DAYS);
	private static Instant hour(long value) {
		return now.plus(value, ChronoUnit.HOURS);
	}
	private static String value(int index) {
		return categories[Math.abs(index) % categories.length];
	}

	private static final String loremIpsum = "lorem ipsum dolor sit amet consectetur adipiscing lorem cras sed dignissim lectus nullam interdum ante vitae lorem";
	private static final String[] categories = loremIpsum.split(" ");
}
