package systems.intino.datamarts.subjectstore.model.signals;

import systems.intino.datamarts.subjectstore.TimeReferences;
import systems.intino.datamarts.subjectstore.model.Signal;

import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.stream.Collectors;

import static systems.intino.datamarts.subjectstore.TimeReferences.iterate;

public interface CategoricalSignal extends Signal<String> {
	default String[] values() { return points().stream().map(Point::value).toArray(String[]::new); }
	default Instant[] instants() { return points().stream().map(Point::instant).toArray(Instant[]::new); }
	default String[] distinct() { return points().stream().map(Point::value).distinct().toArray(String[]::new); }
	default CategoricalSignal[] segments(TemporalAmount duration) { return splitBy(from(), to(), duration); }
	default CategoricalSignal[] segments(int number) { return segments(duration().dividedBy(number)); }
	default Summary summary() { return Summary.of(points()); }

	private Segment[] splitBy(Instant from, Instant to, TemporalAmount duration) {
		return iterate(from, to, duration)
				.map(current -> new Segment(current, TimeReferences.add(current, duration), this))
				.toArray(Segment[]::new);
	}

	final class Raw extends Signal.Raw<String> implements CategoricalSignal {
		public Raw(Instant from, Instant to, List<Point<String>> points) {
			super(from, to, points);
		}
	}

	final class Segment extends Signal.Segment<String> implements CategoricalSignal {

		public Segment(Instant from, Instant to, CategoricalSignal parent) {
			super(from, to, parent);
		}

	}

	class Summary implements Signal.Summary {
		private final int count;
		private final SequencedMap<String, Integer> frequencies;

		public static Summary of(Iterable<Point<String>> points) {
			return new Calculator().calculate(points);
		}

		private Summary(int count, SequencedMap<String, Integer> frequencies) {
			this.count = count;
			this.frequencies = frequencies;
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

		public boolean has(String category) {
			return frequency(category) > 0;
		}

		public int frequency(String category) {
			return frequencies.getOrDefault(category, 0);
		}

		public SequencedMap<String, Integer> frequencies() {
			return frequencies;
		}

		public Map.Entry<String, Integer> highest() {
			return count == 0 ? null : frequencies.sequencedEntrySet().getFirst();
		}

		public Map.Entry<String, Integer> lowest() {
			return count == 0 ? null : frequencies.sequencedEntrySet().getLast();
		}

		public String mode() {
			return count > 0 ? categories().getFirst() : null;
		}

		public double entropy() {
			return count == 0 ? 0 : frequencies.values().stream()
					.mapToDouble(this::entropy)
					.sum();
		}

		private double entropy(long f) {
			return count == 0 ? 0 : entropy((double) f / count);
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
				return new Summary(count, sortedByValue());
			}

			private SequencedMap<String, Integer> sortedByValue() {
				return frequencies.entrySet().stream()
						.sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
			}
		}
	}

	static boolean isCategorical(Signal<?> signal) {
		return signal instanceof CategoricalSignal;
	}

	static boolean hasCategoricalContent(Signal<?> signal) {
		return isCategorical(signal) && !signal.isEmpty();
	}

	static CategoricalSignal categorical(Signal<?> signal) {
		return signal instanceof CategoricalSignal s ? s : null;
	}
}
