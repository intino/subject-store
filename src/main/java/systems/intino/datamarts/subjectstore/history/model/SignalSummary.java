package systems.intino.datamarts.subjectstore.history.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SignalSummary {
	private final int count;
	private final double sum;
	private final double mean;
	private final double sd;
	private final Point<Double> first;
	private final Point<Double> last;
	private final Point<Double> min;
	private final Point<Double> max;

	public static SignalSummary of(Iterable<Point<Double>> points) {
		return new SignalSummary(new Calculator().calculate(points));
	}

	private SignalSummary(Calculator calculator) {
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
		if (!(o instanceof SignalSummary summary)) return false;
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
