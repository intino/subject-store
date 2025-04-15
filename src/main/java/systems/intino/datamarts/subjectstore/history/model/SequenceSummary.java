package systems.intino.datamarts.subjectstore.history.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SequenceSummary {
	private final int count;
	private final Map<String, Integer> frequencies;

	public static SequenceSummary of(Iterable<Point<String>> points) {
		return new Calculator().calculate(points);
	}

	private SequenceSummary(int count, Map<String, Integer> frequencies) {
		this.count = count;
		this.frequencies = Map.copyOf(frequencies);
	}

	public int count() {
		return count;
	}

	public List<String> categories() {
		return frequencies.entrySet().stream()
				.sorted(SequenceSummary::reversed)
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
		if (!(o instanceof SequenceSummary summary)) return false;
		return count == summary.count && Objects.equals(frequencies, summary.frequencies);
	}

	@Override
	public int hashCode() {
		return Objects.hash(count, frequencies);
	}

	private static class Calculator {
		private final Map<String, Integer> frequencies = new HashMap<>();
		private int count = 0;

		public SequenceSummary calculate(Iterable<Point<String>> points) {
			for (Point<String> point : points) {
				frequencies.merge(point.value(), 1, Integer::sum);
				count++;
			}
			return new SequenceSummary(count, frequencies);
		}
	}
}
