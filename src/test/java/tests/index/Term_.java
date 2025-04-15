package tests.index;


import org.junit.Test;
import systems.intino.datamarts.subjectstore.index.model.Term;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class Term_ {
	@Test
	public void should_trim_key_and_value() {
		assertThat(Term.of(" x = 20").tag()).isEqualTo("x");
		assertThat(Term.of(" x = 20").value()).isEqualTo("20");
		assertThat(Term.of(" x = ").tag()).isEqualTo("x");
		assertThat(Term.of(" x = ").value()).isEqualTo("");
	}
}
