package systems.intino.datamarts.subjectstore.model.signals;

import com.tdunning.math.stats.TDigest;
import systems.intino.datamarts.subjectstore.TimeReferences;
import systems.intino.datamarts.subjectstore.model.Signal;

import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public interface NumericalSignal extends Signal<Double> {
	default double[] values() { return points().stream().mapToDouble(Point::value).toArray(); }
	default Instant[] instants() { return points().stream().map(Point::instant).toArray(Instant[]::new); }
	default NumericalSignal[] segments(TemporalAmount duration) { return splitBy(from(), to(), duration); }
	default NumericalSignal[] segments(int number) { return segments(duration().dividedBy(number)); }
	default Summary summary() { return Summary.of(points()); }
	default SignalDistribution distribution() { return SignalDistribution.of(points()); }

	private Segment[] splitBy(Instant from, Instant to, TemporalAmount duration) {
		return TimeReferences.iterate(from, to, duration)
				.map(current -> new Segment(current, TimeReferences.add(current, duration), this))
				.toArray(Segment[]::new);
	}

	final class Raw extends Signal.Raw<Double> implements NumericalSignal {

		public Raw(Instant from, Instant to, List<Point<Double>> points) {
			super(from, to, points);
		}

	}

	final class Segment extends Signal.Segment<Double> implements NumericalSignal {
		public Segment(Instant from, Instant to, NumericalSignal parent) {
			super(from, to, parent);
		}
	}

	class SignalDistribution {
		private final TDigest tdigest;

		public static SignalDistribution of(Iterable<Point<Double>> points) {
			SignalDistribution distribution = new SignalDistribution();
			for (Point<Double> point : points)
				distribution.tdigest.add(point.value());
			distribution.tdigest.compress();
			return distribution;
		}

		private SignalDistribution() {
			this.tdigest = TDigest.createAvlTreeDigest(200);
		}

		public long count() {
			return tdigest.size();
		}

		public double min() {
			return tdigest.getMin();
		}

		public double max() {
			return tdigest.getMax();
		}

		public double probabilityLeftTail(double value) {
			return tdigest.cdf(value);
		}

		public double probabilityRightTail(double value) {
			return 1-tdigest.cdf(value);
		}

		public double quantile(double value) {
			return tdigest.quantile(value);
		}

		public double q1() {
			return quantile(0.25);
		}

		public double q2() {
			return quantile(0.5);
		}

		public double median() {
			return quantile(0.5);
		}

		public double q3() {
			return quantile(0.75);
		}

	}

	class Summary {
		private final int count;
		private final double sum;
		private final double mean;
		private final double sd;
		private final Point<Double> first;
		private final Point<Double> last;
		private final Point<Double> min;
		private final Point<Double> max;

		public static Summary of(Iterable<Point<Double>> points) {
			return new Summary(new Summary.Calculator().calculate(points));
		}

		private Summary(Calculator calculator) {
			this.count = calculator.count;
			this.sum = calculator.sum;
			this.mean = calculator.mean;
			this.sd = calculator.sd();
			this.first = calculator.get("first");
			this.last = calculator.get("last");
			this.min = calculator.get("min");
			this.max = calculator.get("max");
		}

		public Point<Double> first() {
			return first;
		}

		public Point<Double> last() {
			return last;
		}

		public Point<Double> min() {
			return min;
		}

		public Point<Double> max() {
			return max;
		}

		public double range() {
			if (max == null || min == null) return Double.NaN;
			return max.value() - min.value();
		}

		public int count() {
			return count;
		}

		public double sum() {
			return sum;
		}

		public double mean() {
			return mean;
		}

		public double sd() {
			return sd;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof Summary summary)) return false;
			return sum == summary.sum && count == summary.count && Double.compare(mean, summary.mean) == 0 && Double.compare(sd, summary.sd) == 0 && Objects.equals(min, summary.min) && Objects.equals(max, summary.max);
		}

		@Override
		public int hashCode() {
			return Objects.hash(sum, count, mean, sd, min, max);
		}


		private static class Calculator {
			final Map<String, Point<Double>> points;
			double sum = 0;
			double min = Double.MAX_VALUE;
			double max = Double.MIN_VALUE;
			int count = 0;
			double mean = 0;
			double m2 = 0;

			public Calculator() {
				this.points = new HashMap<>();
			}

			private Calculator calculate(Iterable<Point<Double>> points) {
				for (Point<Double> point : points)
					calculate(point);
				return this;
			}

			private void calculate(Point<Double> point) {
				double value = point.value();
				calculate(value);
				if (!points.containsKey("first")) points.put("first", point);
				if (min(value)) points.put("min", point);
				if (max(value)) points.put("max", point);
				points.put("last", point);
			}

			private void calculate(double value) {
				double delta = value - mean;
				count++;
				sum += value;
				mean += delta / count;
				m2 += delta * (value - mean);
			}

			private boolean max(double value) {
				if (value <= max) return false;
				max = value;
				return true;
			}

			private boolean min(double value) {
				if (value >= min) return false;
				min = value;
				return true;
			}

			public double sd() {
				return count > 1 ? Math.sqrt(m2 / (count - 1)) : Double.NaN;
			}

			public Point<Double> get(String name) {
				return points.get(name);
			}
		}
	}

	static boolean isNumerical(Signal<?> signal) {
		return signal instanceof NumericalSignal;
	}

	static boolean hasNumericalContent(Signal<?> signal) {
		return isNumerical(signal) && !signal.isEmpty();
	}

	static NumericalSignal numerical(Signal<?> signal) {
		return signal instanceof NumericalSignal s ? s : null;
	}
}
