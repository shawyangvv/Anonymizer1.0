package databasewrapper;

public interface DatabaseWrapper {
    boolean execute(String sql);
    QueryResult executeQuery(String sql);
    boolean executeInsert(String sql);
    void commit();
    boolean flush();
}
