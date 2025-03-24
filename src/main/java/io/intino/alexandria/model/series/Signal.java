package io.intino.alexandria.model.series;

import io.intino.alexandria.model.Point;
import io.intino.alexandria.model.Series;
import io.intino.alexandria.model.series.signal.Distribution;
import io.intino.alexandria.model.series.signal.Summary;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

public interface Signal extends Series<Long> {
	default Signal normalize(long min, long max) {
		//TODO
		return null;
	}
	default long[] values() { return stream().mapToLong(Point::value).toArray(); }
	default Signal[] segments(Duration duration) { return splitBy(from(), to(), duration); }
	default Signal[] segments(int number) { return segments(duration().dividedBy(number)); }
	default Summary summary() { return Summary.of(this); }
	default Distribution distribution() { return Distribution.of(this); }

	private Segment[] splitBy(Instant from, Instant to, Duration duration) {
		return Stream
				.iterate(from, current -> current.isBefore(to), current -> current.plus(duration))
				.map(current -> new Segment(current, current.plus(duration), this))
				.toArray(Segment[]::new);
	}


	final class Raw extends Series.Raw<Long> implements Signal {

		public Raw(Instant from, Instant to, List<Point<Long>> points) {
			super(from, to, points);
		}

	}

	final class Segment extends Series.Segment<Long> implements Signal {
		private final Signal parent;

		public Segment(Instant from, Instant to, Signal parent) {
			super(from, to, parent);
			this.parent = parent;
		}

		public Signal parent() {
			return parent;
		}

	}
}
