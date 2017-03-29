package anonymizer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.ListIterator;

import databasewrapper.QueryResult;
import databasewrapper.DatabaseWrapper;
import databasewrapper.SqLiteWrapper;

public class AnonRecordTable {
    private Integer[] qidIndices;
    private Integer[] sensIndices;
    private String tableName;

    private DatabaseWrapper databaseWrapper;

    public AnonRecordTable(Integer[] qidIndices, Integer[] sensIndices, String tableName) {
        this.qidIndices = qidIndices;
        this.sensIndices = sensIndices;
        this.tableName = tableName;
        databaseWrapper = SqLiteWrapper.getInstance();
        createTable();
    }

    private void createTable() {
//        String check_SQL = "SELECT NAME FROM SQLITE_MASTER WHERE NAME = '" + tableName + "'";
//        QueryResult result = databaseWrapper.executeQuery(check_SQL);
//        if(result.hasNext()) {
//            String drop_SQL = "DROP TABLE " + tableName;
//            databaseWrapper.execute(drop_SQL);
//           // databaseWrapper.commit();
//        }

        String createTable_SQL = "CREATE TABLE IF NOT EXISTS " + tableName +
                " (RID BIGINT PRIMARY KEY," +
                " EID BIGINT, ";
        for(int i = 0; i < qidIndices.length; i++) {
            createTable_SQL += "ATT_" + qidIndices[i] + " DOUBLE , ";
        }
        for(int i = 0; i < sensIndices.length; i++) {
            createTable_SQL += "ATT_" + sensIndices[i] + " DOUBLE , ";
        }
        createTable_SQL = createTable_SQL.substring(0, createTable_SQL.length() - 2);
        createTable_SQL += ")";
        databaseWrapper.execute(createTable_SQL);
        //databaseWrapper.commit();
    }

    public String getName() {
        return tableName;
    }

    public int getEqSize(long eid) throws SQLException{
        String select_SQL = "SELECT COUNT(*) FROM " + tableName + " WHERE EID = " + Long.toString(eid);
        QueryResult result = databaseWrapper.executeQuery(select_SQL);
        if(result.hasNext()) {
            return ((ResultSet) result.next()).getInt(1);
        } else {
            return 0;
        }
    }

    public double[] parseQiValue(String[] vals, Configuration currConf) throws Exception{
        double[] qiVals = new double[currConf.qidAtts.length];
        for(int i = 0; i < currConf.qidAtts.length; i++) {
            String attVal = vals[currConf.qidAtts[i].index];
            if(currConf.qidAtts[i].catMapInt != null) {
                qiVals[i] = currConf.qidAtts[i].catMapInt.get(attVal);
            } else {
                qiVals[i] = Double.parseDouble(attVal);
            }
        }
        return qiVals;
    }

    public void insert(long eid, double[] qiVals, double[] sensVals) throws SQLException{
        Long rid = new Long(0);
        String select_SQL = "SELECT MAX(RID) FROM " + tableName;
        QueryResult result = databaseWrapper.executeQuery(select_SQL);
        if(result.hasNext()) {
            rid = ((ResultSet) result.next()).getLong(1);
            rid++;
        }

        String insert_SQL = "INSERT INTO " + tableName + " VALUES (" + rid + ", " + eid + ", ";
        for(int i = 0; i < qiVals.length; i++) {
            insert_SQL += qiVals[i] + ", ";
        }
        for(int i = 0; i < sensVals.length; i++) {
            insert_SQL += sensVals[i] + ", ";
        }
        insert_SQL = insert_SQL.substring(0, insert_SQL.length()-2);
        insert_SQL += ")";
        databaseWrapper.execute(insert_SQL);
       // databaseWrapper.commit();
    }


    public boolean checkKAnonymity(int k) throws SQLException{
        String select_SQL = "SELECT COUNT(*) FROM " + tableName
                + " GROUP BY EID ORDER BY COUNT(*) ASC";
        QueryResult result = databaseWrapper.executeQuery(select_SQL);
        while(result.hasNext()) {
            Integer min = ((ResultSet) result.next()).getInt(1);
            System.out.println(min);
            if(min > 0 && min < k) {
                return false;
            } else {
                return true;
            }
        }
        return true;
    }

    public boolean checkKAnonymity(int k, int suppThreshold) throws SQLException {
        String select_SQL = "SELECT COUNT(*) FROM " + tableName
                + " GROUP BY EID ORDER BY COUNT(*) ASC";
        QueryResult result = databaseWrapper.executeQuery(select_SQL);

        int sumForSupp = 0;
        while(result.hasNext()) {
            Integer min = ((ResultSet) result.next()).getInt(1);
            if(min > 0 && min < k) {
                sumForSupp += min;
                if(sumForSupp > suppThreshold) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean checkLDiversity(double l, int sensIndex) throws SQLException {
        String select_SQL = "SELECT COUNT(*), EID FROM " + tableName
                + " GROUP BY ATT_" + Integer.toString(sensIndex) + " , EID"
                + " ORDER BY EID";
        QueryResult result = databaseWrapper.executeQuery(select_SQL);

        long currEID;
        int currSensNum;
        long prevEID = Long.MIN_VALUE;
        ArrayList<Integer> counts = null;
        double sum = 0;

        while(result.hasNext()) {
            ResultSet rs = (ResultSet) result.next();
            currEID = rs.getLong(2);
            currSensNum = rs.getInt(1);

            if(currEID != prevEID) {
                if(counts != null) {
                    double entropy = 0;
                    ListIterator<Integer> iter = counts.listIterator();
                    while(iter.hasNext()) {
                        double prob = iter.next() / sum;
                        entropy += prob * Math.log(prob);
                    }
                    if(-1 * entropy < Math.log(l)) {
                        return false;
                    }
                }

                counts = new ArrayList<Integer>();
                sum = 0;
                prevEID = currEID;
            }
            counts.add(currSensNum);
            sum += currSensNum;
        }

        if(counts != null && counts.size() > 0) {
            double entropy = 0;
            ListIterator<Integer> iter = counts.listIterator();
            while(iter.hasNext()) {
                double prob = iter.next() / sum;
                entropy += prob * Math.log(prob);
            }
            if(-1 * entropy < Math.log(l)) {
                return false;
            }
        }

        return true;
    }

    public boolean checkLDiversity(double l, double c, int sensIndex) throws SQLException {
        String select_SQL = "SELECT COUNT(*), EID FROM " + tableName
                + " GROUP BY ATT_" + Integer.toString(sensIndex) + ", EID"
                + " ORDER BY EID";
        QueryResult result = databaseWrapper.executeQuery(select_SQL);

        long currEID;
        int currSensNum;
        long prevEID = Long.MIN_VALUE;
        ArrayList<Integer> counts = null;

        while(result.hasNext()) {
            ResultSet rs = (ResultSet) result.next();
            currEID = rs.getLong(2);
            currSensNum = rs.getInt(1);

            if(currEID != prevEID) {
                if(counts != null) {
                    if(counts.size() < l) {
                        return false;
                    } else {
                        Integer[] countVals = counts.toArray(new Integer[0]);
                        Arrays.sort(countVals);
                        int r_1 = countVals[countVals.length-1];
                        int sum = 0;
                        for(int i = (int) Math.round(countVals.length - l); i >= 0; i--) {
                            sum += countVals[i];
                        }
                        if(r_1 >= c * sum) {
                            return false;
                        }
                    }
                }
                counts = new ArrayList<Integer>();
                prevEID = currEID;
            }
            counts.add(currSensNum);
        }

        if(counts != null && counts.size() >= 1) {
            Integer[] lastVals = counts.toArray(new Integer[0]);
            Arrays.sort(lastVals);
            int sum=0;
            int r_max = lastVals[lastVals.length-1];

            for(int i = (int) Math.round(lastVals.length - l); i >= 0; i--) {
                sum += lastVals[i];
            }
            if(r_max >= c * sum) {
                return false;
            }
        }

        return true;
    }

    public boolean checkTClosenessCat(double t, int sensIndex, int sensDomSize) throws SQLException {
        if(sensDomSize == 1) {
            return true;
        }
        int[] entireDist = new int[sensDomSize];
        for(int i = 0; i < sensDomSize; i++) {
            entireDist[i] = 0;
        }

        String getDist_SQL = "SELECT COUNT(*), ATT_" + sensIndex + " FROM " + tableName
                + " GROUP BY ATT_" + sensIndex;
        QueryResult result = databaseWrapper.executeQuery(getDist_SQL);

        double entireSize = 0;
        while(result.hasNext()) {
            ResultSet rs = (ResultSet) result.next();
            entireDist[(int) rs.getDouble(2)] = rs.getInt(1);
            entireSize += rs.getInt(1);
        }

        long prevEID = -1;
        int[] currDist = null;
        String iterEqs_SQL = "SELECT EID, COUNT(*), ATT_" + sensIndex
                + " FROM " + tableName
                + " GROUP BY EID, ATT_" + sensIndex
                + " ORDER BY EID";
        result = databaseWrapper.executeQuery(iterEqs_SQL);
        while(result.hasNext()) {
            ResultSet rs = (ResultSet) result.next();
            long currEID = rs.getLong(1);
            int count = rs.getInt(2);
            int attVal = (int) rs.getDouble(3);

            if(prevEID != currEID) {
                if(prevEID != -1) {
                    double currEIDsize = getEqSize(prevEID);
                    double sum = 0;
                    for(int i = 0; i < entireDist.length; i++) {
                        sum += Math.abs(entireDist[i]/entireSize - currDist[i]/currEIDsize);
                    }
                    sum /= 2;
                    if(sum > t) {
                        return false;
                    }
                }

                currDist = new int[sensDomSize];
                for(int i = 0; i < sensDomSize; i++) {
                    currDist[i] = 0;
                }
                prevEID = currEID;
            }
            currDist[attVal] = count;
        }

        if(currDist!= null && prevEID != -1) {
            double sum = 0;
            double eqSize = getEqSize(prevEID);
            for(int i = 0; i < entireDist.length; i++) {
                sum += Math.abs(entireDist[i]/entireSize - currDist[i]/eqSize);
            }
            sum /= 2;
            if(sum > t) {
                return false;
            }
        }
        return true;
    }

    public boolean checkTClosenessNum(double t, int sensIndex) throws SQLException {
        LinkedList<Double[]> entireCounts = new LinkedList<Double[]>();
        String getDist_SQL = "SELECT COUNT(*), ATT_" + sensIndex + " FROM " + tableName
                + " GROUP BY ATT_" + sensIndex + " ORDER BY ATT_" + sensIndex;
        QueryResult result = databaseWrapper.executeQuery(getDist_SQL);

        double entireSize = 0;
        while(result.hasNext()) {
            ResultSet rs = (ResultSet) result.next();
            Double[] entry = new Double[2];
            entry[0] = rs.getDouble(2);
            entry[1] = new Double(rs.getInt(1));
            entireCounts.add(entry);
            entireSize += entry[1];
        }

        String iterEqs_SQL = "SELECT DISTINCT(EID) FROM " + tableName;
        result = databaseWrapper.executeQuery(iterEqs_SQL);
        while(result.hasNext()) {
            long eid = ((ResultSet) result.next()).getLong(1);
            double eqSize = getEqSize(eid);

            getDist_SQL = "SELECT COUNT(*), ATT_" + sensIndex
                    + " FROM " + tableName
                    + " WHERE EID = " + eid
                    + " GROUP BY ATT_" + sensIndex
                    + " ORDER BY ATT_" + sensIndex;
            QueryResult subResult = databaseWrapper.executeQuery(getDist_SQL);

            double sumDist = 0;
            double sumNoAbsolute = 0;
            int m = entireCounts.size();
            double boundary = t * (m - 1);

            ListIterator<Double[]> iter = entireCounts.listIterator();
            while(subResult.hasNext()) {
                Double[] tableCurr = iter.next();
                ResultSet srs = (ResultSet) subResult.next();
                Double eqCurr = srs.getDouble(2);
                int eqCount = srs.getInt(1);

                while(tableCurr[0].compareTo(eqCurr) != 0) {
                    double r_i = tableCurr[1] / entireSize;
                    sumNoAbsolute += r_i;
                    if(sumDist > boundary) {
                        return false;
                    }
                    sumDist += Math.abs(sumNoAbsolute);
                    tableCurr = iter.next();
                }
                double r_i = tableCurr[1] / entireSize - eqCount / eqSize;
                sumNoAbsolute += r_i;
                if(sumDist > boundary) {
                    return false;
                }
                sumDist += Math.abs(sumNoAbsolute);
            }
            while(iter.hasNext()) {
                double r_i = iter.next()[1] / entireSize;
                sumNoAbsolute += r_i;
                if(sumDist > boundary) {
                    return false;
                }
                sumDist += Math.abs(sumNoAbsolute);
            }
        }
        return true;
    }

    public void updateEID(Long oldEID, Long newEID) {
        String update_SQL = "UPDATE " + tableName + " SET EID = " + newEID + " WHERE EID = " + oldEID;
        databaseWrapper.execute(update_SQL);
        //databaseWrapper.commit();
    }

    public void getCopy(AnonRecordTable oldAnon, Long oldEID, Long newEID) {
        String insert_SQL = "INSERT INTO " + this.tableName + " SELECT RID, " +newEID;
        for(int i = 0; i < qidIndices.length; i++) {
            insert_SQL += ", ATT_" + qidIndices[i];
        }
        for(int i = 0; i < sensIndices.length; i++) {
            insert_SQL += ", ATT_" + sensIndices[i];
        }
        insert_SQL += " FROM " + oldAnon.getName() + " WHERE EID = " + oldEID;
        databaseWrapper.execute(insert_SQL);
       // databaseWrapper.commit();
    }

    public void getCut(AnonRecordTable oldAnon, Long oldEID, Long newEID) {
        String insert_SQL = "INSERT INTO " + this.tableName + " SELECT RID, " + newEID;
        for(int i = 0; i < qidIndices.length; i++) {
            insert_SQL += ", ATT_" + qidIndices[i];
        }
        for(int i = 0; i < sensIndices.length; i++) {
            insert_SQL += ", ATT_" + sensIndices[i];
        }
        insert_SQL += " FROM " + oldAnon.getName() + " WHERE EID = " + oldEID;
        databaseWrapper.execute(insert_SQL);
        //databaseWrapper.commit();

        String delete_SQL = "DELETE FROM " + oldAnon.tableName + " WHERE EID = " + oldEID;
        databaseWrapper.execute(delete_SQL);
        //databaseWrapper.commit();
    }


    public void renewRecord(AnonRecordTable that, Long RID, Long newEID) {
        String insert_SQL = "INSERT INTO " + tableName + " SELECT RID, " + newEID;
        for(int i = 0; i < qidIndices.length; i++) {
            insert_SQL += ", ATT_" + qidIndices[i];
        }
        for(int i = 0; i < sensIndices.length; i++) {
            insert_SQL += ", ATT_" + sensIndices[i];
        }
        insert_SQL += " FROM " + that.tableName + " WHERE RID = " + RID;
        databaseWrapper.execute(insert_SQL);
        //databaseWrapper.commit();

        String delete_SQL = "DELETE FROM " + that.tableName + " WHERE RID = " + RID;
        databaseWrapper.execute(delete_SQL);
        //databaseWrapper.commit();
    }

    public void drop() {
        String drop_SQL = "DROP TABLE " + tableName;
        databaseWrapper.execute(drop_SQL);
        //databaseWrapper.commit();
    }
}
