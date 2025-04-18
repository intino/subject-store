package systems.intino.datamarts.subjectstore.view.history;

import java.io.IOException;

public interface FormatReader {
	Format read() throws IOException;
}
