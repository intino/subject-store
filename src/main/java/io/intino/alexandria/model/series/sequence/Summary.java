package io.intino.alexandria.model.series.sequence;

import io.intino.alexandria.model.Point;

import java.util.*;

public class Summary {
	private final int count;
	private final Map<String, Integer> frequencies;

	public static Summary of(Iterable<Point<String>> points) {
		return new Calculator().calculate(points);
	}

	private Summary(int count, Map<String, Integer> frequencies) {
		this.count = count;
		this.frequencies = Map.copyOf(frequencies);
	}

	public int count() {
		return count;
	}

	public List<String> categories() {
		return frequencies.entrySet().stream()
				.sorted(Summary::reversed)
				.map(Map.Entry::getKey)
				.toList();
	}

	private static int reversed(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
		return e2.getValue().compareTo(e1.getValue());
	}

	public int frequency(String category) {
		return frequencies.get(category);
	}

	public String mode() {
		return count > 0 ? categories().get(0) : null;
	}

	public double entropy() {
		return frequencies.values().stream()
				.mapToDouble(this::entropy)
				.sum();
	}

	private double entropy(long f) {
		return entropy((double) f / count);
	}

	private double entropy(double p) {
		return -p * Math.log(p);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Summary summary)) return false;
		return count == summary.count && Objects.equals(frequencies, summary.frequencies);
	}

	@Override
	public int hashCode() {
		return Objects.hash(count, frequencies);
	}

	private static class Calculator {
		private final Map<String, Integer> frequencies = new HashMap<>();
		private int count = 0;

		public Summary calculate(Iterable<Point<String>> points) {
			for (Point<String> point : points) {
				frequencies.merge(point.value(), 1, Integer::sum);
				count++;
			}
			return new Summary(count, frequencies);
		}
	}
}
