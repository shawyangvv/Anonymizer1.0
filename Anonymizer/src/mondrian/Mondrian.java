package mondrian;

import java.sql.ResultSet;

import databasewrapper.QueryResult;
import databasewrapper.SqLiteWrapper;

import anonymizer.AnonRecordTable;
import anonymizer.Anonymizer;
import anonymizer.Configuration;
import anonymizer.EquivalenceTable;
import anonymizer.Interval;

public class Mondrian extends Anonymizer{
    private String[] suppressionValues;

    public Mondrian(Configuration conf) throws Exception{
        super(conf);
        if(conf.k <= 0) {
            throw new Exception("Mondrian: Invalidate parameter k in the configuration file");
        }

        suppressionValues = new String[conf.qidAtts.length];
        for(int i = 0; i < suppressionValues.length; i++) {
            suppressionValues[i] = conf.qidAtts[i].supperssionValue();
        }

        databaseWrapper = SqLiteWrapper.getInstance();

        eqTable = createEquivalenceTable("eq_init");
        anonTable = createAnonRecordsTable("anon_init");
    }

    protected AnonRecordTable createAnonRecordsTable(String tableName) {
        System.out.println("Start to create AnonRecordTable");
        Integer[] qiAtts = new Integer[conf.qidAtts.length];
        for(int i = 0; i < qiAtts.length; i++) {
            qiAtts[i] = conf.qidAtts[i].index;
        }
        return new AnonRecordTable(qiAtts, new Integer[0], tableName);
    }

    protected EquivalenceTable createEquivalenceTable(String tableName) {
        return new EquivalenceTable(conf.qidAtts, tableName);
    }

    protected long insertTupleToEquivalenceTable(String[] vals) throws Exception{
        Long eid = eqTable.getEID(suppressionValues);
        if(eid.compareTo(new Long(-1)) == 0) {
            eid = eqTable.insertEquivalence(suppressionValues);
        }
        return eid;
    }

    protected void insertTupleToAnonTable(String[] vals, long eid) throws Exception{
        Configuration currConf=this.conf;
        double[] qiVals = anonTable.parseQiValue(vals, currConf);

        anonTable.insert(eid, qiVals, new double[0]);
    }

    public void anonymize() throws Exception {
        AnonRecordTable readyRecords = createAnonRecordsTable("anon_result");
        EquivalenceTable readyEqs = createEquivalenceTable("eq_result");

        int numUnprocessedEqs = 1;
        while(numUnprocessedEqs > 0) {
            String getEID_SQL = "SELECT EID FROM " + eqTable.getName();
            QueryResult result = databaseWrapper.executeQuery(getEID_SQL);
            Long eid = ((ResultSet) result.next()).getLong(1);
            result.__close();

            String[] genVals = eqTable.getGeneralization(eid);
            String currentGenVals = genVals[0];
            for(int i = 1; i < genVals.length; i++) {
                currentGenVals += ", " + genVals[i] + " ";
            }
            System.out.println("Processing EID = " + eid + " with generalized values: " + currentGenVals);

            int dim = -1;
            double maxNormWidth = 0.0;
            double splitVal = Double.NaN;
            Double[] medians = new Double[conf.qidAtts.length];
            for(int i = 0; i < conf.qidAtts.length; i++) {
                medians[i] = getMedian(eid, conf.qidAtts[i].index);
                if(medians[i] != null) {
                    double normWidth=getNormalizedWidth(genVals[i], suppressionValues[i]);
                    if (normWidth > maxNormWidth){
                        maxNormWidth=normWidth;
                        dim=i;
                        splitVal=medians[dim];
                    }
                }
            }

            if(dim != -1) {
                System.out.println("Current dim="+ dim);
                System.out.print("Splitting "+ genVals[dim]);
                Interval oldRange = new Interval(genVals[dim]);
                Interval[] newRange = oldRange.splitInterval(splitVal);
                String[] lhsGenVals = genVals.clone();
                lhsGenVals[dim] = newRange[0].toString();
                long lhsEID = eqTable.insertEquivalence(lhsGenVals);
                updateEIDs(eid, lhsEID, newRange[0], conf.qidAtts[dim].index);

                String[] rhsGenVals = genVals;
                rhsGenVals[dim] = newRange[1].toString();
                long rhsEID = eqTable.insertEquivalence(rhsGenVals);
                updateEIDs(eid, rhsEID, newRange[1], conf.qidAtts[dim].index);

                eqTable.deleteEquivalence(eid);
                numUnprocessedEqs += 1;
                System.out.println(" to LHS " + newRange[0] + " insert as EID " + lhsEID + " and "
                                     + "RHS " + newRange[1] + " insert as EID " + rhsEID );
            } else {
                long newEid = readyEqs.insertEquivalence(genVals);
                readyRecords.getCut(anonTable, eid, newEid);
                eqTable.deleteEquivalence(eid);

                numUnprocessedEqs --;
                System.out.println("Record " + eid + " has no allowable cuts");
            }
        }

        if(!readyRecords.checkKAnonymity(conf.k)) {
            throw new Exception("This table cannot be anonymized at k = " + conf.k);
        } else {
            this.anonTable.drop();
            this.eqTable.drop();
            this.anonTable = readyRecords;
            this.eqTable = readyEqs;
        }


    }

    private double getNormalizedWidth(String curr, String entire) throws Exception{
        Interval entireRange = new Interval(entire);
        double entireWidth = entireRange.high - entireRange.low;

        Interval currRange = new Interval(curr);
        double currWidth = currRange.high - currRange.low;
        if(currWidth == 0 && currRange.singleton()) {
            currWidth = 1;
        }

        return currWidth / entireWidth;
    }

    private void updateEIDs(long oldEID, long newEID, Interval value, int dim) {
        String update_SQL = "UPDATE " + anonTable.getName()
                + " SET EID = " + newEID
                + " WHERE EID = " + oldEID + " AND " + value.checkInDB("ATT_" + dim);
        databaseWrapper.execute(update_SQL);
    }

    public Double getMedian(long eid, int dim) throws Exception{
        String iterVals_SQL = "SELECT ATT_" + dim + ", COUNT(*)" + " FROM " + anonTable.getName()
                            + " WHERE EID = " + eid
                            + " GROUP BY ATT_" + dim
                            + " ORDER BY ATT_" + dim;
        QueryResult result = databaseWrapper.executeQuery(iterVals_SQL);

        double oldSize = anonTable.getEqSize(eid);
        double currSize = 0;
        double mid = 0;
        while(result.hasNext()) {
            ResultSet rs = (ResultSet) result.next();
            currSize += rs.getInt(2);
            if(currSize >= oldSize / 2) {
                mid = rs.getDouble(1);
                result.__close();
                break;
            }
        }

        if(currSize >= conf.k && (oldSize - currSize) >= conf.k) {
            result.__close();
            return mid;
        } else {
            result.__close();
            return null;
        }
    }
}
