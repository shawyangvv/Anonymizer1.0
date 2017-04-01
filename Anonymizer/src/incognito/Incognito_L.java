package incognito;

import java.util.LinkedList;
import java.util.ListIterator;

import java.sql.ResultSet;
import databasewrapper.QueryResult;
import databasewrapper.SqLiteWrapper;

import anonymizer.AnonRecordTable;
import anonymizer.Anonymizer;
import anonymizer.Configuration;
import anonymizer.EquivalenceTable;

public class Incognito_L extends Anonymizer {
    private HierarchyGraph man;

    public Incognito_L(Configuration conf) throws Exception{
        super(conf);
        int[] dghDepths = null;

        if(conf.l <= 1) {
            throw new Exception("Incognito_L: the value of 'l' is invalidate.");
        }
        if(conf.c <= 0) {
            throw new Exception("Incognito_L: the value of 'c' is invalidate.");
        }
        if(conf.sensitiveAtts.length != 1) {
            throw new Exception("Incognito_L: Only 1 sensitive attribute could be set.");
        }

        dghDepths = new int[conf.qidAtts.length];
        for(int i = 0; i < dghDepths.length; i++) {
            dghDepths[i] = conf.qidAtts[i].getVGHDepth(true);
        }

        int[] root = new int[conf.qidAtts.length];
        for(int i = 0; i < root.length; i++) {
            root[i] = 0;
        }
        GraphNode superRoot = new GraphNode(root);
        man = new HierarchyGraph(superRoot, dghDepths);

        databaseWrapper = SqLiteWrapper.getInstance();
        eqTable = createEquivalenceTable("eq_init");
        anonTable = createAnonRecordsTable("anon_init");
    }

    protected EquivalenceTable createEquivalenceTable(String tableName) {
        return new EquivalenceTable(conf.qidAtts, tableName);
    }

    protected AnonRecordTable createAnonRecordsTable(String tableName) {
        Integer[] qiAtts = new Integer[conf.qidAtts.length];
        for(int i = 0; i < qiAtts.length; i++) {
            qiAtts[i] = conf.qidAtts[i].index;
        }
        Integer[] sensAtts = new Integer[1];
        sensAtts[0] = conf.sensitiveAtts[0].index;
        return new AnonRecordTable(qiAtts, sensAtts, tableName);
    }

    protected long insertTupleToEquivalenceTable(String[] vals) throws Exception{
        return eqTable.insertTuple(vals);
    }


    protected void insertTupleToAnonTable(String[] vals, long eid) throws Exception{
        Configuration currConf=this.conf;
        double[] qiVals = new double[currConf.qidAtts.length];
        qiVals = anonTable.parseQiValue(vals, currConf);

        String sensitiveValue = vals[conf.sensitiveAtts[0].index];
        double[] sensVals = new double[1];
        if(conf.sensitiveAtts[0].catMapInt != null) {
            sensVals[0] = conf.sensitiveAtts[0].catMapInt.get(sensitiveValue);
        } else {
            sensVals[0] = Double.parseDouble(sensitiveValue);
        }
        anonTable.insert(eid, qiVals, sensVals);
    }

    public void anonymize() throws Exception{
        man.nextNode();
        if(checkL(anonTable)) {
            man.setResult(true, anonTable, eqTable);
        } else {
            man.setResult(false, null, null);
        }

        while(man.hasNextNode()) {
            GraphNode currRoot = man.nextNode();
            generalize(currRoot);
        }

        selectAnonymization(man.getSatisfiedNodes());
    }

    public boolean checkL(AnonRecordTable table) throws Exception{
        if(conf.c <= 0) {
            return table.checkLDiversity(conf.l, conf.sensitiveAtts[0].index);
        } else {
            return table.checkLDiversity(conf.l, conf.c, conf.sensitiveAtts[0].index);
        }
    }

    private void generalize(GraphNode currRoot) throws Exception {
        System.out.println("Start to create eq_" + currRoot.toString()+ " Table");
        EquivalenceTable currEqTable = createEquivalenceTable("eq_" + currRoot.toString());
        System.out.println("Start to create anon_" + currRoot.toString() + " Table");
        AnonRecordTable currAnonTable = createAnonRecordsTable("anon_" + currRoot.toString());

        EquivalenceTable oldEqTable = eqTable;
        AnonRecordTable oldAnonTable = anonTable;

        currAnonTable = man.generalizeVals(currRoot, conf, currEqTable, currAnonTable, oldEqTable, oldAnonTable);

        if(checkL(currAnonTable)) {
            man.setResult(true, currAnonTable, currEqTable);
        } else {
            currAnonTable.drop();
            currEqTable.drop();
            man.setResult(false, null, null);
        }
    }

    private void selectAnonymization(LinkedList<GraphNode> anons) throws Exception{
        System.out.println(anons.size() +" nodes satisfy l-diversity requirement");
        GraphNode selection = man.nextNode();
        selection = selection.chooseNode(anons);

        if(selection == null) {
            throw new Exception("No satisfied anonymous generalizations of Incognito_L.");
        } else {
            eqTable = selection.eqTable;
            anonTable = selection.anonTable;

            System.out.println("Selection: " + selection.toString());
        }
    }
}
