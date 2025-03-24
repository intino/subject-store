package tests;

import io.intino.alexandria.model.Point;
import org.junit.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class Point_ {
	@Test
	public void should_generate_name() {
		assertThat(new Point<>(0, Instant.parse("2025-01-01T01:02:03Z"), "4").toString()).isEqualTo("[20250101010203=4]");
	}
}
