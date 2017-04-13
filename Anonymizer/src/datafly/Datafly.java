package datafly;

import java.sql.ResultSet;
import java.util.LinkedList;

import databasewrapper.QueryResult;
import databasewrapper.SqLiteWrapper;
import anonymizer.AnonRecordTable;
import anonymizer.Anonymizer;
import anonymizer.Configuration;
import anonymizer.EquivalenceTable;

public class Datafly extends Anonymizer{

    public boolean fullDomainGeneralization = true;
    private int eqTableIndex;
    private int anonTableIndex;

    public Datafly(Configuration conf) throws Exception{
        super(conf);

        if(conf.k <= 0) {
            throw new Exception("Datafly: Parameter k is invalid!");
        }
        for(int i = 0; i < conf.qidAtts.length; i++) {
            if(!conf.qidAtts[i].checkVGH()) {
                throw new Exception("Datafly: the leaf nodes are not at the same depth.");
            }
        }
        suppressionThreshold = conf.k;
        eqTableIndex = 1;
        anonTableIndex = 1;

        databaseWrapper = SqLiteWrapper.getInstance();
        eqTable = createEquivalenceTable("eq_" + eqTableIndex);
        anonTable = createAnonRecordsTable("anon_" + anonTableIndex);
    }

    protected EquivalenceTable createEquivalenceTable(String tableName) {
        return new EquivalenceTable(conf.qidAtts, tableName);
    }

    protected AnonRecordTable createAnonRecordsTable(String tableName) {
        Integer[] qiAtts = new Integer[conf.qidAtts.length];
        for(int i = 0; i < qiAtts.length; i++) {
            qiAtts[i] = conf.qidAtts[i].index;
        }
        return new AnonRecordTable(qiAtts, new Integer[0], tableName);
    }

    protected long insertTupleToEquivalenceTable(String[] vals) throws Exception{
        return eqTable.insertTuple(vals);
    }

    protected void insertTupleToAnonTable(String[] vals, long eid) throws Exception{
        double[] qiVals = anonTable.parseQiValue(vals, conf);
        anonTable.insert(eid, qiVals, new double[0]);
    }

    public void anonymize() throws Exception{
        LinkedList<Long> suppressionList;

        String checkSize_SQL = "SELECT COUNT(*) FROM " + anonTable.getName();
        int totalSize = ((ResultSet) databaseWrapper.executeQuery(checkSize_SQL).next()).getInt(1);
        if(totalSize < conf.k) {
            throw new Exception("This input cannot be anonymized at k = " + conf.k);
        }

        while((suppressionList = checkSuppression(anonTable.getName())) == null) {
            int[] genDomainCounts = new int[conf.qidAtts.length];
            for(int i = 0; i < genDomainCounts.length; i++) {
                String count_SQL = "SELECT COUNT(*) FROM "
                        + "(SELECT COUNT(*) FROM " + eqTable.getName() + " GROUP BY"
                        + " ATT_" + conf.qidAtts[i].index + ") AS T";
                QueryResult result = databaseWrapper.executeQuery(count_SQL);
                genDomainCounts[i] = ((ResultSet) result.next()).getInt(1);
            }

            int genAttribute = 0;
            int maxSize = genDomainCounts[genAttribute];
            for(int i = 1; i < genDomainCounts.length; i++) {
                if(genDomainCounts[i] > maxSize) {
                    genAttribute = i;
                    maxSize = genDomainCounts[i];
                }
            }

            System.out.println("Generalizing attribute " + conf.qidAtts[genAttribute].index);
            String newEqvTableName = "eq_" + (++eqTableIndex);
            EquivalenceTable newEqTable = createEquivalenceTable(newEqvTableName);
            String newAnonTableName = "an_" + (++anonTableIndex);
            AnonRecordTable newAnTable = createAnonRecordsTable(newAnonTableName);

            String iterateEquivalences = "SELECT EID FROM " + eqTable.getName();
            QueryResult result = databaseWrapper.executeQuery(iterateEquivalences);
            while(result.hasNext()) {
                ResultSet rs = (ResultSet) result.next();
                Long oldEID = rs.getLong(1);
                double count = anonTable.getEqSize(oldEID);

                String[] genVals = eqTable.getGeneralization(oldEID);
                if(count < conf.k || fullDomainGeneralization) {
                    genVals[genAttribute] = conf.qidAtts[genAttribute].findGeneralization(genVals[genAttribute]);
                }

                Long newEID = newEqTable.getEID(genVals);
                if(newEID.compareTo(new Long(-1)) == 0) {
                    newEID = newEqTable.insertEquivalence(genVals);
                }
                newAnTable.getCopy(anonTable, oldEID, newEID);
            }
//            anonTable.drop();
//            eqTable.drop();
            eqTable = newEqTable;
            anonTable = newAnTable;
        }

        suppress(suppressionList);
    }
}