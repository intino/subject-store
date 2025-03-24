package io.intino.alexandria.model.table.operators;

import io.intino.alexandria.model.series.Sequence;

import java.util.function.Function;

public interface CategoricalOperator extends Function<Sequence, Object> {
	CategoricalOperator Count = Sequence::count;
	CategoricalOperator Entropy = s -> s.isEmpty() ? "" : s.summary().entropy();
	CategoricalOperator Mode = s -> s.isEmpty() ? "" : s.summary().mode();

}
