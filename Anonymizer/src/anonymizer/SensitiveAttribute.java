package anonymizer;

import java.util.Hashtable;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class SensitiveAttribute {
    public Hashtable<String, Integer> catMapInt = null;
    public int index = -1;

    public SensitiveAttribute(int index, Node att) throws Exception {
        this.index = index;

        NodeList childList = att.getChildNodes();
        for(int i = 0; i < childList.getLength(); i++) {
            Node child = childList.item(i);
            if(child.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            else if(child.getNodeName().compareTo("map") == 0) {
                parseMapping(child);
            }
        }
    }

    private void parseMapping(Node map) throws Exception{
        catMapInt = new Hashtable<String, Integer>();

        NodeList childList = map.getChildNodes();
        for(int i = 0; i < childList.getLength(); i++) {
            Node child = childList.item(i);
            if(child.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            NamedNodeMap nodeAtts = child.getAttributes();
            String key = null;
            int value = -1;
            Node n = nodeAtts.getNamedItem("cat");
            if(n == null) {
                throw new Exception("Cannot parse the mapping of the sensitive attribute at index" + index);
            } else {
                key = n.getNodeValue();
            }
            n = nodeAtts.getNamedItem("int");
            if(n == null) {
                throw new Exception("Cannot parse the mapping of the sensitive attribute at index" + index);
            } else {
                value = Integer.parseInt(n.getNodeValue());
            }
            catMapInt.put(key, value);
        }
    }
}
