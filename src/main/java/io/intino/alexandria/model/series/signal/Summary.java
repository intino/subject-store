package io.intino.alexandria.model.series.signal;

import io.intino.alexandria.model.Point;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Summary {
	private final long sum;
	private final int count;
	private final double mean;
	private final double sd;
	private final Point<Long> first;
	private final Point<Long> last;
	private final Point<Long> min;
	private final Point<Long> max;

	public static Summary of(Iterable<Point<Long>> points) {
		return new Summary(new Calculator().calculate(points));
	}

	private Summary(Calculator calculator) {
		this.sum = calculator.sum;
		this.count = calculator.count;
		this.mean = calculator.mean;
		this.sd = calculator.sd();
		this.first = calculator.get("first");
		this.last = calculator.get("last");
		this.min = calculator.get("min");
		this.max = calculator.get("max");
	}

	public Point<Long> first() {
		return first;
	}

	public Point<Long> last() {
		return last;
	}

	public Point<Long> min() {
		return min;
	}

	public Point<Long> max() {
		return max;
	}

	public long range() {
		return max.value() - min.value();
	}

	public long sum() {
		return sum;
	}

	public int count() {
		return count;
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
		final Map<String, Point<Long>> points;
		long sum = 0;
		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		int count = 0;
		double mean = 0;
		double m2 = 0;

		public Calculator() {
			this.points = new HashMap<>();
		}

		private Calculator calculate(Iterable<Point<Long>> points) {
			for (Point<Long> point : points)
				calculate(point);
			return this;
		}

		private void calculate(Point<Long> point) {
			long value = point.value();
			calculate(value);
			if (!points.containsKey("first")) points.put("first", point);
			if (min(value)) points.put("min", point);
			if (max(value)) points.put("max", point);
			points.put("last", point);
		}

		private void calculate(long value) {
			double delta = value - mean;
			count++;
			sum += value;
			mean += delta / count;
			m2 += delta * (value - mean);
		}

		private boolean max(long value) {
			if (value <= max) return false;
			max = value;
			return true;
		}

		private boolean min(long value) {
			if (value >= min) return false;
			min = value;
			return true;
		}

		public double sd() {
			return count > 1 ? Math.sqrt(m2 / (count - 1)) : Double.NaN;
		}

		public Point<Long> get(String name) {
			return points.get(name);
		}
	}
}
