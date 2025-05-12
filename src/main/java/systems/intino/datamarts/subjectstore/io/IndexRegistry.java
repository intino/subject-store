package systems.intino.datamarts.subjectstore.io;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public interface IndexRegistry extends AutoCloseable {
	List<String> subjects();
	List<Long> links();
	List<String> terms(List<Integer> ids);
	List<String> terms(Set<String> tags);

	int insertSubject(String subject);
	void setSubject(int id, String subject);
	void deleteSubject(int subject);

	int term(String tag, String value);
	int insertTerm(String tag, String value);
	void setTerm(int id, String tag, String value);
	void deleteTerm(int term);

	void link(int subject, int term);
	void unlink(int subject, int term);

	void commit();

	Stream<String> dump();

	boolean hasHistory(int id);

}
