package anonymizer;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.ListIterator;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class QIDAttribute {
    public Hashtable<String,Integer> catMapInt = null;
    public Hashtable<String, String[]> intToCat = null;
    private Hashtable<String,String> parentLookup = null;
    private Hashtable<String,String[]> childLookup = null;
    private LinkedList<Interval> leaves = null;
    public int index = -1;

    private String suppValue = null;

    public QIDAttribute(int index, Node att) throws Exception {
        this.index = index;
        parentLookup = new Hashtable<String, String>();
        childLookup = new Hashtable<String, String[]>();
        leaves = new LinkedList<Interval>();

        NodeList childList = att.getChildNodes();
        for(int i = 0; i < childList.getLength(); i++) {
            Node child = childList.item(i);
            if(child.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            else if(child.getNodeName().compareTo("map") == 0) {
                parseMapping(child);
            } else if(child.getNodeName().compareTo("vgh") == 0) {
                parseVGH(child);
            }
        }

        if(suppValue == null) {
            throw new Exception("Cannot parse the VGH of qid-attribute at index " + index);
        }
    }

    private void parseMapping(Node catmap) throws Exception{
        catMapInt = new Hashtable<String, Integer>();

        NodeList childList = catmap.getChildNodes();
        for(int i = 0; i < childList.getLength(); i++) {
            Node child = childList.item(i);
            if(child.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            NamedNodeMap nodeAtts = child.getAttributes();
            String catvalue= null;
            int intvalue = -1;
            Node n = nodeAtts.getNamedItem("cat");
            if(n == null) {
                throw new Exception("Cannot parse the mapping of the qid-attribute at index" + index);
            } else {
                catvalue= n.getNodeValue();
            }
            n = nodeAtts.getNamedItem("int");
            if(n == null) {
                throw new Exception("Cannot parse the mapping of the qid-attribute at index" + index);
            } else {
                intvalue = Integer.parseInt(n.getNodeValue());
            }

            catMapInt.put(catvalue, intvalue);
        }
    }

    private void parseVGH(Node vgh) throws Exception{
        Node temp = vgh.getAttributes().getNamedItem("value");
        if(temp == null) {
            throw new Exception("Error in VGH structure, attribute " + index + "does not have value!");
        }
        suppValue = temp.getNodeValue();

        LinkedList<Node> unprocessedNodes = new LinkedList<Node>();
        unprocessedNodes.add(vgh);
        while(!unprocessedNodes.isEmpty()) {
            Node n = unprocessedNodes.removeFirst();
            temp = n.getAttributes().getNamedItem("value");
            if(temp == null) {
                throw new Exception("Error in VGH structure, unsuppressed attribute " + index + "does not have value!");
            }
            String parent = temp.getNodeValue();
            Interval parInt = new Interval(parent);

            LinkedList<String> children = new LinkedList<String>();
            NodeList childNodes = n.getChildNodes();
            for(int i = 0; i < childNodes.getLength(); i++) {
                Node child = childNodes.item(i);
                if(child.getNodeType() == Node.ELEMENT_NODE) {
                    unprocessedNodes.add(child);
                    temp = child.getAttributes().getNamedItem("value");
                    if(temp == null) {
                        throw new Exception("Error in VGH structure, children attribute " + index + "does not have value");
                    }
                    String value = temp.getNodeValue();
                    parentLookup.put(value, parent);
                    children.add(value);
                    if(!parInt.contains(new Interval(value))) {
                        throw new Exception("Error in VGH structure, parent attribute " + index + "does not have value");
                    }
                }
            }

            if(!children.isEmpty()) {
                String[] childNames = children.toArray(new String[0]);
                childLookup.put(parent, childNames);
            } else {
                leaves.add(parInt);
            }
        }

        if(catMapInt != null) {
            Enumeration<String> catValue = catMapInt.keys();
            while(catValue.hasMoreElements()) {
                int val = catMapInt.get(catValue.nextElement());
                String gen = findGeneralization(Integer.toString(val));
                if(gen == null) {
                    throw new Exception("Malformed VGH, " + val + " cannot be generalized to any value!!!");
                }
            }
        }
    }

    public boolean checkVGH() {
        int treeDepth = -1;
        ListIterator<Interval> hierarchies = leaves.listIterator();
        while(hierarchies.hasNext()) {
            if(treeDepth == -1) {
                treeDepth = getDepth(hierarchies.next().toString());
            } else if(treeDepth != getDepth(hierarchies.next().toString())) {
                return false;
            }
        }
        return true;
    }

    public int getVGHDepth(boolean validateDGH) throws Exception{
        if(validateDGH) {
            if(!checkVGH()) {
                throw new Exception("The DGH has no corresponding VGH!");
            }
        }

        int depth = getDepth(leaves.peek().toString());
        if(leaves.peek().singleton()) {
            return depth;
        } else {
            return depth + 1;
        }
    }

    private int getDepth(String leaf) {
        int depth = 0;
        while(leaf.compareTo(suppValue) != 0) {
            depth++;
            leaf = findGeneralization(leaf);
        }
        return depth;
    }

    public String supperssionValue() {
        return suppValue;
    }

    public String findGeneralization(String value) {
        if(value.compareTo(suppValue) == 0) {
            return value;
        }

        String parent = parentLookup.get(value);
        if(parent != null) {
            return parent;
        }

        for(int i = 0; i < leaves.size(); i++) {
            Interval leaf = leaves.get(i);
            if(leaf.belongTo(value)) {
                return leaf.toString();
            }
        }
        return null;
    }

    public String[] getLeavesValue(String value) throws Exception{
        String[] children = childLookup.get(value);
        if(children != null) {
            return children;
        }

        if(catMapInt != null) {
            Interval intrvl = new Interval(value);
            LinkedList<String> specs = new LinkedList<String>();
            for(int i = (int) intrvl.low; i < intrvl.high; i++) {
                specs.addLast(Integer.toString(i));
            }

            if(intrvl.incType == Interval.TYPE_ExcLowExcHigh) {
                specs.removeFirst();
                specs.removeLast();
            } else if(intrvl.incType == Interval.TYPE_ExcLowIncHigh) {
                specs.removeFirst();
            } else if(intrvl.incType == Interval.TYPE_IncLowExcHigh) {
                specs.removeLast();
            } else {
                if(intrvl.low == intrvl.high) {
                    specs.removeFirst();
                }
            }
            return specs.toArray(new String[0]);
        } else {
            Interval i = new Interval(value);
            if(i.incType == Interval.TYPE_IncLowIncHigh
                    && i.low == i.high) {
                String[] retVal = new String[1];
                retVal[0] = value;
                return retVal;
            } else {
                throw new Exception("Cannot find the higher granularity of "+value);
            }
        }
    }

    public boolean checkGeneralization(String value, String generalization) throws Exception{
        Interval gen = new Interval(generalization);
        return gen.belongTo(value);
    }


    public String[] getCategory() {
        return getCategory();
    }

    public String[] mapBack(String val) {
        if(catMapInt == null) {
            return null;
        }
        if(intToCat == null) {
            intToCat = new Hashtable<String, String[]>();
            Enumeration<String> enu = catMapInt.keys();
            while(enu.hasMoreElements()) {
                String key = enu.nextElement();
                LinkedList<String> seq = new LinkedList<String>();
                seq.add(key);
                double doubleVal = catMapInt.get(key);
                String parent = "[" + doubleVal + "]";
                while((parent = findGeneralization(parent)) != null) {
                    if(parent.compareTo(suppValue) == 0) {
                        break;
                    }
                }

                String[] genSeq = seq.toArray(new String[0]);
                intToCat.put(key, genSeq);
                intToCat.put("[" + Double.toString(doubleVal) + "]", genSeq);
            }

            LinkedList<String> unprocessed = new LinkedList<String>();
            unprocessed.add(suppValue);
            while(!unprocessed.isEmpty()) {
                String curr = unprocessed.removeFirst();
                String[] children = childLookup.get(curr);
                for(int i = 0; children != null && i < children.length; i++) {
                    unprocessed.add(children[i]);
                }

                LinkedList<String> seq = new LinkedList<String>();
                seq.add(curr);
                String parent = curr;
                while((parent = findGeneralization(parent)).compareTo(curr) != 0) {
                    seq.add(parent);
                    if(parent.compareTo(suppValue) == 0) {
                        break;
                    }
                }
                String[] genSeq =seq.toArray(new String[0]);
                intToCat.put(curr, genSeq);
            }
        }
        if(intToCat.containsKey(val)) {
            return intToCat.get(val);
        }
        return null;
    }

    public int[] getCategory(String val) {
        try {
            int lowInc, highInc;
            Interval range = new Interval(val);
            if(range.incType == Interval.TYPE_IncLowExcHigh || range.incType == Interval.TYPE_IncLowIncHigh) {
                lowInc = (int) range.low;
            } else {
                lowInc = (int) range.low + 1;
            }
            if(range.incType == Interval.TYPE_ExcLowIncHigh || range.incType == Interval.TYPE_IncLowIncHigh) {
                highInc = (int) range.high;
            } else {
                highInc = (int) range.high - 1;
            }
            int[] retVal = new int[highInc - lowInc + 1];
            for(int i = lowInc; i <= highInc; i++) {
                retVal[i-lowInc] = i;
            }
            return retVal;
        } catch(Exception e) {
            int[] retVal = new int[1];
            retVal[0] = catMapInt.get(val);
            return retVal;
        }
    }
}
