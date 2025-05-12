package systems.intino.datamarts.subjectstore.view.history.format;

import java.io.IOException;

public interface HistoryFormatReader {
	HistoryFormat read() throws IOException;
}
