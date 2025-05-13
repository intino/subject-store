package systems.intino.datamarts.subjectstore.model;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public interface Signal<T>  {
	Instant from();
	Instant to();
	List<Point<T>> points();
	default Duration duration() { return Duration.between(from(), to()); }

	int count();
	boolean isEmpty();


	abstract class Raw<X> implements Signal<X> {
		private final Instant from;
		private final Instant to;
		private final List<Point<X>> points;

		public Raw(Instant from, Instant to, List<Point<X>> points) {
			this.from = from;
			this.to = to;
			this.points = points;
		}

		@Override
		public Instant from() {
			return from;
		}

		@Override
		public Instant to() {
			return to;
		}

		@Override
		public int count() {
			return points().size();
		}

		@Override
		public boolean isEmpty() {
			return points.isEmpty();
		}

		public List<Point<X>> points() {
			return points;
		}

		@Override
		public int hashCode() {
			return Objects.hash(from, to, points);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof Raw<?> raw)) return false;
			return Objects.equals(from, raw.from) && Objects.equals(to, raw.to) && Objects.equals(points, raw.points);
		}
	}

	abstract class Segment<X> implements Signal<X> {
		private final Instant from;
		private final Instant to;
		private final int fromIndex;
		private final int toIndex;
		protected final Signal<X> parent;

		public Segment(Instant from, Instant to, Signal<X> parent) {
			this.from = from;
			this.to = to;
			this.parent = parent;
			this.fromIndex = fromIndex();
			this.toIndex = toIndex();
		}

		public Instant from() {
			return from;
		}

		public Instant to() {
			return to;
		}

		public int count() {
			return toIndex - fromIndex;
		}

		public boolean isEmpty() {
			return count() <= 0;
		}

		public List<Point<X>> points() {
			return parent.points().subList(fromIndex, toIndex);
		}

		private int fromIndex() {
			return find(from);
		}

		private int toIndex() {
			return find(to);
		}

		private int find(Instant point) {
			List<Point<X>> points = parent.points();
			int low = 0;
			int high = parent.count();
			while (low < high) {
				int mid = (low + high) / 2;
				if (points.get(mid).instant().isBefore(point))
					low = mid + 1;
				else
					high = mid;
			}
			return low;
		}

		@Override
		public int hashCode() {
			return Objects.hash(from, to, parent);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof Segment<?> segment)) return false;
			return Objects.equals(from, segment.from) && Objects.equals(to, segment.to) && Objects.equals(parent, segment.parent);
		}
	}

	record Point<T>(int feed, Instant instant, T value) {
		@Override
		public String toString() {
			return "[" + clean(instant) + "=" + value + ']';
		}

		private String clean(Instant instant) {
			return instant.toString().replaceAll("[-T:Z]","");
		}
	}
}
