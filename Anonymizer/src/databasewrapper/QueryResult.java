package databasewrapper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import databasewrapper.SqLiteWrapper;

public class QueryResult implements Iterator{
    private String query;
    private Statement stat;
    private ResultSet currentPage;

    private int currentRecordId=0;
    private int currentPageId = 0;
    private ArrayList currentRecord = new ArrayList();
    private int recordSum;
    private int numCols=0;
    private int pageSize = 10000;
    private int numberOfPages;
    private boolean pageLoaded = false;

    private Statement __statement  = null;

    public QueryResult(Statement stat, String sql) throws SQLException{
        this.query = sql;
        this.stat = stat;



        ResultSet rs = stat.executeQuery("select count(*) as recordCount from ( " + sql + " )");
        if(rs.next())
            recordSum = rs.getInt(1);
        numberOfPages = recordSum/ pageSize;
    }

    public boolean hasNext() {
        if(currentRecordId>= recordSum) {
            try {
//                stat.close();
                if(currentPage != null)
                    currentPage.close();
            } catch(SQLException e) {
                e.printStackTrace();
            }
            return false;
        }
        return true;
    }

    public Object next() {
        currentRecord.clear();
        try {
            if (currentPage == null || !currentPage.next())
                nextPage();
        } catch (SQLException e1) {	e1.printStackTrace();}

        if (currentPage != null) {
            if (pageLoaded) {
                try {
                    currentPage.next();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                pageLoaded = false;
            }

            currentRecordId++;
            return currentPage;
        }
        return  null;
    }

    public void nextPage() throws SQLException {
        int startIndex;
        int endIndex;

        if(currentPageId <= numberOfPages) {
            startIndex = currentPageId*pageSize ;

            endIndex = startIndex + pageSize;

            if (endIndex > recordSum+1)
                endIndex = recordSum + 1 ;

            String pageQuery;
            if(query.contains(" limit 1 offset ")) {
                pageQuery = query;
            } else {
                pageQuery = query + " limit " + (endIndex- startIndex)+ " offset " +  startIndex;
            }
            if(__statement != null && !__statement.isClosed()){
                __statement.close();
            }
            __statement = SqLiteWrapper.getInstance().__getConnection().createStatement();
            currentPage = __statement.executeQuery(pageQuery);
            if (numCols == 0) {
                ResultSetMetaData rsmd = currentPage.getMetaData();
                numCols = rsmd.getColumnCount();
            }

            currentPageId++;
            pageLoaded = true;

        }else
            currentPage = null;
    }

    public void setPageSize(int pageSize){
        this.pageSize = pageSize;
    }

    public void remove() {
    }

    public void __close() throws SQLException {
        if (__statement != null && ! __statement.isClosed())
             __statement.close();
    }
}
