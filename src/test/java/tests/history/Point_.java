package tests.history;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.model.Signal;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class Point_ {
	@Test
	public void should_generate_name() {
		assertThat(new Signal.Point<>(0, Instant.parse("2025-01-01T01:02:03Z"), "4").toString()).isEqualTo("[20250101010203=4]");
	}
}
