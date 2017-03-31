package incognito;

import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.ListIterator;

import databasewrapper.QueryResult;
import databasewrapper.SqLiteWrapper;
import anonymizer.AnonRecordTable;
import anonymizer.Anonymizer;
import anonymizer.Configuration;
import anonymizer.EquivalenceTable;

public class Incognito_K extends Anonymizer {
    public int suppressionThreshold;
    private HierarchyGraph currGraph;

    public Incognito_K(Configuration conf) throws Exception{
        super(conf);
        int[] qiHierarchies = null;

        if(conf.k <= 0) {
            throw new Exception("Incognito_K: Parameter k is invalidate.");
        }

        qiHierarchies = new int[conf.qidAtts.length];
        for(int i = 0; i < qiHierarchies.length; i++) {
            qiHierarchies[i] = conf.qidAtts[i].getVGHDepth(true);
        }

        int[] root = new int[conf.qidAtts.length];
        for(int i = 0; i < root.length; i++) {
            root[i] = 0;
        }
        GraphNode superRoot = new GraphNode(root);
        currGraph = new HierarchyGraph(superRoot, qiHierarchies);

        databaseWrapper = SqLiteWrapper.getInstance();
        System.out.println("Start to create eq_init Table");
        eqTable = createEquivalenceTable("eq_init");
        System.out.println("Start to create anon_init Table");
        anonTable = createAnonRecordsTable("an_init");
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
        Configuration currConf=this.conf;
        double[] qiVals = anonTable.parseQiValue(vals, currConf);
        anonTable.insert(eid, qiVals, new double[0]);
    }

    public void anonymize() throws Exception{
        currGraph.nextNode();
        if(check(anonTable)) {
            currGraph.setResult(true, anonTable, eqTable);
        } else {
            currGraph.setResult(false, null, null);
        }

        while(currGraph.hasNextNode()) {
            GraphNode currRoot = currGraph.nextNode();
            generalize(currRoot);
        }

        selectNode(currGraph.getSatisfiedNodes());
    }

    private boolean check(AnonRecordTable table) throws Exception{
        return table.checkKAnonymity(conf.k, suppressionThreshold);
    }

    private void generalize(GraphNode currRoot) throws Exception {
        System.out.println("Start to create eq_" + currRoot.toString()+ " Table");
        EquivalenceTable currEqTable = createEquivalenceTable("eq_" + currRoot.toString());
        System.out.println("Start to create anon_" + currRoot.toString() + " Table");
        AnonRecordTable currAnonTable = createAnonRecordsTable("anon_" + currRoot.toString());

        EquivalenceTable oldEqTable = eqTable;
        AnonRecordTable oldAnonTable = anonTable;

        currAnonTable = currGraph.generalizeVals(currRoot, conf, currEqTable, currAnonTable, oldEqTable, oldAnonTable);

        if(check(currAnonTable)) {
            currGraph.setResult(true, currAnonTable, currEqTable);
        } else {
//            currAnonTable.drop();
//            currEqTable.drop();
            currGraph.setResult(false, null, null);
        }
    }

    private void selectNode(LinkedList<GraphNode> anons) throws Exception{
        int equivalencesNum = 0;
        GraphNode selection = null;
        ListIterator<GraphNode> candidates = anons.listIterator();
        LinkedList<Long> selectionSuppList = null;
        LinkedList<Long> suppressionList = null;

        System.out.println(anons.size()+" nodes satisfies Incognito_K requirements:");

        while(candidates.hasNext()) {
            try {
                GraphNode currRoot = candidates.next();
                System.out.println(currRoot.toString());
                String count_SQL = "SELECT COUNT(*) FROM " + currRoot.eqTable.getName();
                QueryResult result = databaseWrapper.executeQuery(count_SQL);
                int currEqNum = ((ResultSet) result.next()).getInt(1);
                suppressionList = checkSuppression(currRoot.anonTable.getName());
                currEqNum -= suppressionList.size();

                if(currEqNum > equivalencesNum) {
                    selection = currRoot;
                    equivalencesNum = currEqNum;
                    selectionSuppList = suppressionList;
//                } else {
//                    currRoot.anonTable.drop();
//                    currRoot.eqTable.drop();
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        if(selection == null) {
            throw new Exception("None generalizations satisfy Incognito_K.");
        } else {
            System.out.println("Selection: " + selection.toString());
            eqTable = selection.eqTable;
            anonTable = selection.anonTable;
            suppress(selectionSuppList);
        }
    }
}

