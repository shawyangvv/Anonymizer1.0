package anonymizer;

import java.sql.ResultSet;
import java.sql.SQLException;

import databasewrapper.QueryResult;
import databasewrapper.DatabaseWrapper;
import databasewrapper.SqLiteWrapper;

public class EquivalenceTable {
    private QIDAttribute[] qid;
    private String tableName;
    private DatabaseWrapper databaseWrapper;

    public EquivalenceTable(QIDAttribute[] qid, String tableName) {
        this.qid = qid;
        this.tableName = tableName;
        databaseWrapper = SqLiteWrapper.getInstance();
        createTable();

    }

    private void createTable() {
//        String select_SQL = "SELECT NAME FROM SQLITE_MASTER WHERE NAME = '" + tableName + "'";
//        QueryResult result = databaseWrapper.executeQuery(select_SQL);
//        if(result.hasNext()) {
//            String drop_SQL = "DROP TABLE " + tableName;
//            databaseWrapper.execute(drop_SQL);
//            //databaseWrapper.commit();
//        }

        System.out.println("Start to create Equivalence Table");

        String createTable_SQL = "CREATE TABLE IF NOT EXISTS " + tableName +
                " (EID BIGINT PRIMARY KEY, ";
        for(int i = 0; i < qid.length; i++) {
            createTable_SQL += "ATT_" + qid[i].index + " VARCHAR(128), ";
        }
        createTable_SQL = createTable_SQL.substring(0, createTable_SQL.length() - 2);
        createTable_SQL += ")";
        databaseWrapper.execute(createTable_SQL);
        //databaseWrapper.commit();
    }

    public String getName() {
        return tableName;
    }

    public Long getEID(String[] genVals) throws SQLException{
        if(genVals.length != qid.length) {
            return new Long(-1);
        }

        String select_SQL = "SELECT EID FROM " + tableName + " WHERE ";
        for(int i = 0; i < qid.length; i++) {
            select_SQL += "ATT_" + qid[i].index + " ='" + genVals[i] + "' AND ";
        }
        select_SQL = select_SQL.substring(0, select_SQL.length()-4);
        QueryResult result = databaseWrapper.executeQuery(select_SQL);

        if(result.hasNext()) {
            return ((ResultSet) result.next()).getLong(1);
        } else {
            return new Long(-1);
        }
    }

    public String[] getGeneralization(long eid) throws SQLException{
        String select_SQL = "SELECT * FROM " + tableName + " WHERE EID = " + eid;
        QueryResult result = databaseWrapper.executeQuery(select_SQL);

        if(result.hasNext()) {
            String[] retVal = new String[qid.length];
            ResultSet rs = (ResultSet) result.next();
            for(int i = 0; i < retVal.length; i++) {
                retVal[i] = rs.getString(i+2);
            }
            return retVal;
        } else {
            return null;
        }
    }

    public void updateGeneralization(double eid, String[] newVals) throws SQLException {
        String update_SQL = "UPDATE " + tableName + " SET ";
        update_SQL += "ATT_" + qid[0].index + " = '" + newVals[0] + "'";
        for(int i = 1; i < qid.length; i++) {
            update_SQL += ", ATT_" + qid[i].index + " = '" + newVals[i] + "'";
        }
        update_SQL += " WHERE EID = " + eid;
        databaseWrapper.execute(update_SQL);
       // databaseWrapper.commit();
    }

    public Long insertTuple(String[] vals) throws Exception {
        String[] genVals = new String[qid.length];
        for(int i =0; i < genVals.length; i++) {
            String attVal = vals[qid[i].index];
            if(qid[i].catMapInt != null) {
                attVal = qid[i].catMapInt.get(attVal).toString();
            }
            genVals[i] = new Interval(attVal).toString();
        }
        Long eid = getEID(genVals);
        if(eid.compareTo(new Long(-1)) == 0) {
            eid = insertEquivalence(genVals);
        }
        return eid;
    }

    public Long insertEquivalence(String[] genVals) throws SQLException{
        Long eid = new Long(0);
        String select_SQL = "SELECT MAX(EID) FROM " + tableName;
        QueryResult result = databaseWrapper.executeQuery(select_SQL);
        if(result.hasNext()) {
            eid = ((ResultSet) result.next()).getLong(1);
            eid++;
        }
        //System.out.println(tableName);
        String insert_SQL = "INSERT INTO " + tableName + " VALUES ( " + eid.toString() + ", ";
        for(int i = 0; i < genVals.length; i++) {
            insert_SQL += "'" + genVals[i] + "', ";
        }
        insert_SQL = insert_SQL.substring(0, insert_SQL.length()-2);
        insert_SQL += ")";
        databaseWrapper.execute(insert_SQL);
        //databaseWrapper.commit();

        return eid;
    }

    public void deleteEquivalence(Long eid) {
        String delete_SQL = "DELETE FROM " + tableName + " WHERE EID = " + eid;
        databaseWrapper.execute(delete_SQL);
        //databaseWrapper.commit();
    }

    public void drop() {
        String drop_SQL = "DROP TABLE " + tableName;
        databaseWrapper.execute(drop_SQL);
       // databaseWrapper.commit();
    }
}
