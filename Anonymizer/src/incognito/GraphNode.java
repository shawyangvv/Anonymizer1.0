package incognito;

import anonymizer.AnonRecordTable;
import anonymizer.EquivalenceTable;
import databasewrapper.DatabaseWrapper;
import databasewrapper.QueryResult;
import databasewrapper.SqLiteWrapper;

import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.ListIterator;

public class GraphNode {
    public int currIndex;
    private int[] currValue;

    public AnonRecordTable anonTable;
    public EquivalenceTable eqTable;

    protected DatabaseWrapper databaseWrapper;

    public GraphNode(int[] value) {
        this.currValue = value;
        this.currIndex = 0;
    }

    public GraphNode(GraphNode parent, int currIndex) {
        this.currIndex = currIndex;

        this.currValue = parent.currValue.clone();
        this.currValue[this.currIndex]++;
    }

    public boolean checkParent(GraphNode parent) {
        if(this.currValue.length != parent.currValue.length) {
            try {
                throw new Exception("The two entry have different qi-attribute.");
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        for(int i = 0; i < this.currValue.length; i++) {
            if(this.currValue[i] > parent.currValue[i]) {
                return false;
            }
        }
        return true;
    }

    public int getHierarchy(int index) {
        return currValue[index];
    }

    public String getDirectParent() {
        int[] parentRoot = this.currValue.clone();
        parentRoot[currIndex]--;

        String parent = Integer.toString(parentRoot[0]);
        for(int i = 1; i < parentRoot.length; i++) {
            parent += "_" + parentRoot[i];
        }
        return parent;
    }

    public void updateTables(AnonRecordTable anonTable, EquivalenceTable eqTable) {
        this.anonTable = anonTable;
        this.eqTable = eqTable;
    }

    public String toString() {
        String retVal = Integer.toString(currValue[0]);
        for(int i = 1; i < currValue.length; i++) {
            retVal += "_" + Integer.toString(currValue[i]);
        }
        return retVal;
    }

}
