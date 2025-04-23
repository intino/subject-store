package systems.intino.datamarts.subjectstore.io;

import java.util.List;
import java.util.stream.Stream;

public interface IndexRegistry extends AutoCloseable {
	List<String> subjects();
	List<String> terms();

	List<Integer> fullTermsOf(int subject);
	List<Integer> selfTermsOf(int subject);

	List<Integer> subjectsWithAll(List<Integer> terms);
	List<Integer> subjectsWithAny(List<Integer> terms);

	int insertSubject(String subject);
	void setSubject(int id, String subject);
	void deleteSubject(int subject);

	int insertTerm(String term);
	void setTerm(int id, String term);
	void deleteTerm(int term);

	void link(int subject, int term);
	void unlink(int subject, int term);

	void commit();

	Stream<String> dump();

	boolean hasHistory(int id);
}
