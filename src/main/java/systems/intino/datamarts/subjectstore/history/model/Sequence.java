package systems.intino.datamarts.subjectstore.history.model;

import systems.intino.datamarts.subjectstore.TimeReference;

import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.List;

import static systems.intino.datamarts.subjectstore.TimeReference.iterate;

public interface Sequence extends Series<String> {
	default String[] values() { return stream().map(Point::value).toArray(String[]::new); }
	default String[] distinct() { return stream().map(Point::value).distinct().toArray(String[]::new); }
	default Sequence[] segments(TemporalAmount duration) { return splitBy(from(), to(), duration); }
	default Sequence[] segments(int number) { return segments(duration().dividedBy(number)); }
	default SequenceSummary summary() { return SequenceSummary.of(this); }

	private Segment[] splitBy(Instant from, Instant to, TemporalAmount duration) {
		return iterate(from, to, duration)
				.map(current -> new Segment(current, TimeReference.add(current, duration), this))
				.toArray(Segment[]::new);
	}

	final class Raw extends Series.Raw<String> implements Sequence {
		public Raw(Instant from, Instant to, List<Point<String>> points) {
			super(from, to, points);
		}
	}

	final class Segment extends Series.Segment<String> implements Sequence {

		public Segment(Instant from, Instant to, Sequence parent) {
			super(from, to, parent);
		}

	}
}
