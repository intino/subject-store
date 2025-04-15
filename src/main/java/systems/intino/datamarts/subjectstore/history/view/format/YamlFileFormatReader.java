package systems.intino.datamarts.subjectstore.history.view.format;

import systems.intino.datamarts.subjectstore.history.view.Format;
import systems.intino.datamarts.subjectstore.history.view.FormatReader;

import java.io.*;

public class YamlFileFormatReader implements FormatReader {
	private final File file;

	public YamlFileFormatReader(File file) {
		this.file = file;
	}

	@Override
	public Format read() throws IOException {
		try (InputStream input = new BufferedInputStream(new FileInputStream(file))) {
			return new YamlFormatReader(new String(input.readAllBytes())).read();
		}
	}



}
