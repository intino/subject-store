package io.intino.alexandria.model.series;

import io.intino.alexandria.model.Point;
import io.intino.alexandria.model.Series;
import io.intino.alexandria.model.series.sequence.Summary;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

public interface Sequence extends Series<String> {
	default String[] values() { return stream().map(Point::value).toArray(String[]::new); }
	default Sequence[] segments(Duration duration) { return splitBy(from(), to(), duration); }
	default Sequence[] segments(int number) { return segments(duration().dividedBy(number)); }
	default Summary summary() { return Summary.of(this); }

	private Segment[] splitBy(Instant from, Instant to, Duration duration) {
		return Stream
				.iterate(from, current -> current.isBefore(to), current -> current.plus(duration))
				.map(current -> new Segment(current, current.plus(duration), this))
				.toArray(Segment[]::new);
	}

	final class Raw extends Series.Raw<String> implements Sequence {
		public Raw(Instant from, Instant to, List<Point<String>> points) {
			super(from, to, points);
		}
	}

	final class Segment extends Series.Segment<String> implements Sequence {
		private final Sequence parent;

		public Segment(Instant from, Instant to, Sequence parent) {
			super(from, to, parent);
			this.parent = parent;
		}

		public Sequence parent() {
			return parent;
		}

	}
}
