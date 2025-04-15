package systems.intino.datamarts.subjectstore.history.view;

import java.io.IOException;

public interface FormatReader {
	Format read() throws IOException;
}
