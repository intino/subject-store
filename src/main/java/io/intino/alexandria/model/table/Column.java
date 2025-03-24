package io.intino.alexandria.model.table;

import io.intino.alexandria.model.series.Sequence;
import io.intino.alexandria.model.series.Signal;
import io.intino.alexandria.model.table.operators.CategoricalFunction;
import io.intino.alexandria.model.table.operators.NumericalFunction;
import io.intino.alexandria.model.table.operators.TemporalFunction;

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

	record Numerical(String name, NumericalFunction function) implements Column {

		@Override
		public Type type() {
			return Type.Numerical;
		}

		public Object apply(Signal signal) {
			return function.apply(signal);
		}

	}

	record Categorical(String name, CategoricalFunction function) implements Column {

		@Override
		public Type type() {
			return Type.Categorical;
		}

		public Object apply(Sequence sequence) {
			return function.apply(sequence);
		}
	}

	enum Type {
		Temporal, Numerical, Categorical
	}

}
