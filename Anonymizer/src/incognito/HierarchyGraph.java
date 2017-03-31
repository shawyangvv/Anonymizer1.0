package incognito;

import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.ListIterator;

import anonymizer.AnonRecordTable;
import anonymizer.Anonymizer;
import anonymizer.Configuration;
import anonymizer.EquivalenceTable;
import databasewrapper.DatabaseWrapper;
import databasewrapper.QueryResult;
import databasewrapper.SqLiteWrapper;

public class HierarchyGraph {
    private int[] qiHierarchies = null;
    private GraphNode[] unreachedRoots = null;
    private int nextNodeIndex = 0;
    private GraphNode prevNode;
    private LinkedList<GraphNode> satisfiedNodes;
    protected EquivalenceTable eqTable;
    protected AnonRecordTable anonTable;
    protected DatabaseWrapper databaseWrapper;

    public HierarchyGraph(GraphNode superRoot, int[] qiHierarchies) {
        this.qiHierarchies = qiHierarchies;

        unreachedRoots = new GraphNode[1];
        unreachedRoots[0] = superRoot;
        nextNodeIndex = 0;

        satisfiedNodes = new LinkedList<GraphNode>();
    }

    public boolean hasNextNode() {
        if (nextNodeIndex < unreachedRoots.length) {
            return true;
        } else {
            nextNodeIndex = 0;
            LinkedList<GraphNode> newRoots = new LinkedList<GraphNode>();
            for (int i = 0; i < unreachedRoots.length; i++) {
                if (unreachedRoots[i] == null) {
                    continue;
                }
                GraphNode curr = unreachedRoots[i];
                for (int j = curr.currIndex; j < qiHierarchies.length; j++) {
                    GraphNode newConf = new GraphNode(curr, j);
                    if (newConf.getHierarchy(j) <= qiHierarchies[j]) {
                        newRoots.add(newConf);
                    }
                }
            }
            if (newRoots.isEmpty()) {
                return false;
            }

            unreachedRoots = newRoots.toArray(new GraphNode[0]);
            return true;
        }
    }


    public LinkedList<GraphNode> getSatisfiedNodes() {
        return satisfiedNodes;
    }

    public GraphNode nextNode() {
        prevNode = unreachedRoots[nextNodeIndex];
        return prevNode;
    }

    public void setResult(boolean flag, AnonRecordTable anonTable, EquivalenceTable eqTable) {
        if (flag) {
            prevNode.updateTables(anonTable, eqTable);
            satisfiedNodes.add(prevNode);
            unreachedRoots[nextNodeIndex] = null;
        } else {
            System.out.println("Generalization at Node <" + prevNode.toString() + "> has failed.");
        }
        nextNodeIndex++;
    }

    protected AnonRecordTable generalizeVals(GraphNode currRoot, Configuration conf, EquivalenceTable currEqTable, AnonRecordTable currAnonTable, EquivalenceTable oldEqTable, AnonRecordTable oldAnonTable) throws Exception {
        databaseWrapper = SqLiteWrapper.getInstance();

        String iterateEquivalences = "SELECT EID FROM " + oldEqTable.getName();
        QueryResult result = databaseWrapper.executeQuery(iterateEquivalences);

        while (result.hasNext()) {
            ResultSet rs = (ResultSet) result.next();
            Long oldEID = rs.getLong(1);
            String[] genVals = oldEqTable.getGeneralization(oldEID);

            for (int i = 0; i < genVals.length; i++) {
                for (int j = 0; j < currRoot.getHierarchy(i); j++) {
                    genVals[i] = conf.qidAtts[i].findGeneralization(genVals[i]);
                }
            }

            Long newEID = currEqTable.getEID(genVals);
            if (newEID.compareTo(new Long(-1)) == 0) {
                newEID = currEqTable.insertEquivalence(genVals);
            }
            currAnonTable.getCopy(oldAnonTable, oldEID, newEID);
        }
        return currAnonTable;
    }




}
