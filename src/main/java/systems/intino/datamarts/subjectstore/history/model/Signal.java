package systems.intino.datamarts.subjectstore.history.model;

import systems.intino.datamarts.subjectstore.TimeReference;

import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.List;

public interface Signal extends Series<Double> {
	default double[] values() { return stream().mapToDouble(Point::value).toArray(); }
	default Signal[] segments(TemporalAmount duration) { return splitBy(from(), to(), duration); }
	default Signal[] segments(int number) { return segments(duration().dividedBy(number)); }
	default SignalSummary summary() { return SignalSummary.of(this); }
	default SignalDistribution distribution() { return SignalDistribution.of(this); }

	private Segment[] splitBy(Instant from, Instant to, TemporalAmount duration) {
		return  TimeReference.iterate(from, to, duration)
				.map(current -> new Segment(current, TimeReference.add(current, duration), this))
				.toArray(Segment[]::new);
	}


	final class Raw extends Series.Raw<Double> implements Signal {

		public Raw(Instant from, Instant to, List<Point<Double>> points) {
			super(from, to, points);
		}

	}

	final class Segment extends Series.Segment<Double> implements Signal {
		public Segment(Instant from, Instant to, Signal parent) {
			super(from, to, parent);
		}

	}
}
