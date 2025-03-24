package io.intino.alexandria.model.table;

import io.intino.alexandria.model.Point;
import io.intino.alexandria.model.series.Sequence;
import io.intino.alexandria.model.series.Signal;
import io.intino.alexandria.model.table.operators.CategoricalOperator;
import io.intino.alexandria.model.table.operators.NumericalFunction;
import io.intino.alexandria.model.table.operators.TemporalFunction;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public interface Column {
	String name();
	Type type();

	record Temporal(TemporalFunction operator) implements Column {
		@Override
		public String name() {
			return "";
		}

		@Override
		public Type type() {
			return Type.Temporal;
		}


	}

	final class Numerical implements Column {
		private final String name;
		private final NumericalFunction function;
		private final Normalization normalization;

		public Numerical(String name, Normalization normalization, NumericalFunction function) {
			this.name = name;
			this.normalization = normalization;
			this.function = function;
		}

		public Numerical(String name, NumericalFunction function) {
			this(name, Normalization.None, function);
		}

		@Override
		public String name() {
			return name;
		}

		@Override
		public Type type() {
			return Type.Numerical;
		}

		public Object apply(Signal signal) {
			return function.apply(signal);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) return true;
			if (obj == null || obj.getClass() != this.getClass()) return false;
			var that = (Numerical) obj;
			return Objects.equals(this.name, that.name) &&
				   Objects.equals(this.normalization, that.normalization) &&
				   Objects.equals(this.function, that.function);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, normalization, function);
		}

		@Override
		public String toString() {
			return "Numerical[" +
				   "name=" + name + ", " +
				   "normalization=" + normalization + ", " +
				   "function=" + function + ']';
		}

		public Signal normalize(Signal signal) {
			return normalization.apply(signal);
		}

		public interface Normalization extends Function<Signal, Signal> {
			Normalization None = s -> s;
			Normalization MinMax = Normalization::minMax;

			private static Signal minMax(Signal signal) {
				long range = signal.summary().range();
				List<Point<Long>> points = signal.stream().map(p -> new Point<>(p.feed(), p.instant(), (long) (p.value() * 100_000. / range))).toList();
				return new Signal.Raw(signal.from(), signal.to(), points);
			}
		}
	}

	record Categorical(String name, CategoricalOperator operator) implements Column {

		@Override
		public Type type() {
			return Type.Categorical;
		}

		public Object apply(Sequence sequence) {
			return operator.apply(sequence);
		}
	}

	public enum Type {
		Temporal, Numerical, Categorical
	}

}
