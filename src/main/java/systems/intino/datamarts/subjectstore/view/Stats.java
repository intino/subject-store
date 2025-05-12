package systems.intino.datamarts.subjectstore.view;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Stats {
	private final int count;
	private final Map<String, Integer> frequencies;

	public static Stats of(String[] values) {
		return new FrequencyCalculator().calculate(values);
	}

	public Stats(int count, Map<String, Integer> frequencies) {
		this.count = count;
		this.frequencies = frequencies;
	}

	public List<String> categories() {
		return frequencies.entrySet().stream()
				.sorted(Stats::reversed)
				.map(Map.Entry::getKey)
				.toList();
	}

	public int frequency(String category) {
		return frequencies.get(category);
	}

	public String mode() {
		return count > 0 ? categories().getFirst() : null;
	}

	public double entropy() {
		return frequencies.values().stream()
				.mapToDouble(this::entropy)
				.sum();
	}

	private static int reversed(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
		return e2.getValue().compareTo(e1.getValue());
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
		if (!(o instanceof Stats stats)) return false;
		return count == stats.count && Objects.equals(frequencies, stats.frequencies);
	}

	@Override
	public int hashCode() {
		return Objects.hash(count, frequencies);
	}

	public static class FrequencyCalculator {
		private final Map<String, Integer> frequencies = new HashMap<>();
		private int count = 0;

		public Stats calculate(String[] values) {
			for (String value : values) {
				frequencies.merge(value, 1, Integer::sum);
				count++;
			}
			return new Stats(count, frequencies);
		}
	}
}
