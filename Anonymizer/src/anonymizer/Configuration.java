package anonymizer;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.LinkedList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.*;

public class Configuration {
    public static final int METHOD_MONDRIAN = 1;
    public static final int METHOD_INCOGNITO_K = 2;
    public static final int METHOD_INCOGNITO_L = 3;
    public static final int METHOD_INCOGNITO_T = 4;
    public static final int METHOD_DATAFLY=5;
    public int anonMethod = METHOD_MONDRIAN;

    public int suppressionThreshold = 10;

    public int k = 10;
    public double l = 10;
    public double c = 0.2;
    public double t = 0.2;

    public String configFilename = null;
    public String inputFilename = null;
    public String separator = ",";
    public String outputFilename = null;

    public LinkedList<Integer> kidAtts = new LinkedList<Integer>();
    public SensitiveAttribute[] sensitiveAtts = null;
    public QIDAttribute[] qidAtts = null;

    public String sqlitefilePath = null;

    public Configuration(String configFile) throws Exception {
        this.configFilename = configFile;
        File f = new File(configFilename);
        if(!f.exists()) {
            throw new Exception("Configuration file " + configFilename + " does not exist!");
        }

        inputFilename = null;
        outputFilename = null;

        parseConfigFile(configFilename);
        checkValidity();
    }

    public Configuration(String[] args) throws Exception {
        int index = getOption("-config", args);
        if(index >= 0) {
            configFilename = args[index];
        } else {
            configFilename = "config.xml";
        }

        File f = new File(configFilename);
        if(!f.exists()) {
            throw new Exception("Configuration file " + configFilename + " does not exist!");
        }

        parseConfigFile(configFilename);
        setOptions(args);
        checkValidity();
    }

    private void parseConfigFile(String configFilename) throws Exception{
        DocumentBuilderFactory domfac = DocumentBuilderFactory.newInstance();
        DocumentBuilder dombuilder = domfac.newDocumentBuilder();
        InputStream input = new FileInputStream(configFilename);
        Document doc = dombuilder.parse(input);

        Node root = doc.getFirstChild();
        while(root.getNodeType() != Node.ELEMENT_NODE) {
            root = root.getNextSibling();
            if(root == null) {
                throw new Exception("Empty configuration file.");
            }
        }

        NamedNodeMap atts = root.getAttributes();
        for(int j = 0; j < atts.getLength(); j++) {
            String attName = atts.item(j).getNodeName();
            if (attName.equals("k")){
                this.k = Integer.parseInt(atts.item(j).getNodeValue());
            } else if (attName.equals("l")){
                this.l = Integer.parseInt(atts.item(j).getNodeValue());
            } else if (attName.equals("c")){
                this.c = Double.parseDouble(atts.item(j).getNodeValue());
            } else if (attName.equals("t")){
                this.t = Double.parseDouble(atts.item(j).getNodeValue());
            } else if (attName.equals("method")){
                setMethod(atts.item(j).getNodeValue());
            } else {
                throw new Exception("Unrecognized configuration parameter "+ attName);
            }
        }

        Node kid = null;
        Node qid = null;
        Node sens = null;
        NodeList rootChildren = root.getChildNodes();
        for(int i = 0; i < rootChildren.getLength(); i++) {
            Node child = rootChildren.item(i);
            if(child.getNodeType() == Node.ELEMENT_NODE) {
                if (child.getNodeName().equals("input")){
                    NamedNodeMap nodeAtts = child.getAttributes();
                    for(int j = 0; j < nodeAtts.getLength(); j++) {
                        String attName = nodeAtts.item(j).getNodeName();
                        if (attName.equals("filename")){
                            setInputFilename(nodeAtts.item(j).getNodeValue());
                        } else if(attName.equals("separator")) {
                            this.separator = nodeAtts.item(j).getNodeValue();
                        }
                    }
                } else if(child.getNodeName().equals("output")) {
                    NamedNodeMap nodeAtts = child.getAttributes();
                    for(int j = 0; j < nodeAtts.getLength(); j++) {
                        String attName = nodeAtts.item(j).getNodeName();
                        if(attName.equals("filename")) {
                            setOutputFilename(nodeAtts.item(j).getNodeValue());
                            //System.out.println(nodeAtts.item(j).getNodeValue());
                        }
                    }
                } else if (child.getNodeName().equals("sqlitefile")){
                    NamedNodeMap nodeAtts = child.getAttributes();
                    for (int j = 0; j < nodeAtts.getLength(); j++){
                        String attName = nodeAtts.item(j).getNodeName();
                        if (attName.equals("path")){
                            setSqlitefilePath(nodeAtts.item(j).getNodeValue());
                        }
                    }
                } else if(child.getNodeName().equals("kid")) {
                    kid = child;
                    parseKidAtts(kid);
                } else if(child.getNodeName().equals("qid")) {
                    qid = child;
                    parseQidAtts(qid);
                } else if(child.getNodeName().equals("sens")) {
                    sens = child;
                    parseSensAtts(sens);
                }
            }
        }

        if(qid == null) {
            throw new Exception("No quasi-identifier.");
        }
    }

    private void parseKidAtts(Node id) throws Exception{
        NodeList kidList = id.getChildNodes();
        for(int i = 0; i < kidList.getLength(); i++) {
            Node att = kidList.item(i);
            if(att.getNodeType() != Node.ELEMENT_NODE){
                continue;
            }
            NamedNodeMap nodeAtts = att.getAttributes();
            Node n = nodeAtts.getNamedItem("index");
            if(n != null) {
                int index = Integer.parseInt(n.getNodeValue());
                kidAtts.add(index);
            }
        }
    }

    private void parseQidAtts(Node qid) throws Exception{
        LinkedList<QIDAttribute> qidAttsList = new LinkedList<QIDAttribute>();
        NodeList qidList = qid.getChildNodes();
        for(int i = 0; i < qidList.getLength(); i++) {
            Node att = qidList.item(i);
            if(att.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            NamedNodeMap nodeAtts = att.getAttributes();
            Node n = nodeAtts.getNamedItem("index");
            if(n == null) {
                throw new Exception("No Quasi-identifier attributes");
            }
            int index = Integer.parseInt(n.getNodeValue());
            qidAttsList.add(new QIDAttribute(index, att));
        }
        qidAtts = qidAttsList.toArray(new QIDAttribute[0]);
    }

    private void parseSensAtts(Node sens) throws Exception{
        LinkedList<SensitiveAttribute> sensitiveAttsList = new LinkedList<SensitiveAttribute>();
        NodeList sensList = sens.getChildNodes();
        for(int i = 0; i < sensList.getLength(); i++) {
            Node att = sensList.item(i);
            if(att.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            NamedNodeMap nodeAtts = att.getAttributes();
            Node n = nodeAtts.getNamedItem("index");
            if(n == null) {
                throw new Exception("Sensitive attribute has no index!");
            }
            int index = Integer.parseInt(n.getNodeValue());
            sensitiveAttsList.add(new SensitiveAttribute(index, att));
        }
        sensitiveAtts = sensitiveAttsList.toArray(new SensitiveAttribute[0]);
    }

    public void setInputFilename(String filename) throws Exception{
        inputFilename = filename;
    }

    public void setOutputFilename(String filename) throws Exception {
        File outputFile = new File(filename);
        File theDirectory = outputFile.getParentFile();
        if (theDirectory != null) {
            theDirectory.mkdirs();
        }

        outputFilename = filename;
    }

    public void setSqlitefilePath(String path) {
        sqlitefilePath = path;
    }

    public void setMethod(String method){
        if(method.equals("Mondrian")) {
            this.anonMethod = METHOD_MONDRIAN;
        } else if(method.equals("Incognito_K")) {
            this.anonMethod = METHOD_INCOGNITO_K;
        } else if (method.equals("Incognito_L")){
            this.anonMethod = METHOD_INCOGNITO_L;
        } else if (method.equals("Incognito_T")){
            this.anonMethod = METHOD_INCOGNITO_T;
        } else if (method.equals("DataFly")){
            this.anonMethod = METHOD_DATAFLY;
        } else {
            System.out.println("WARNING: Unrecognized anonymization method " + method + ", Mondrian will be used for anonymization");
            this.anonMethod = METHOD_MONDRIAN;
        }
    }

    public void setOptions(String[] args) {
        int index = -1;
        if( (index = getOption("-k", args)) >= 0) {
            k = Integer.parseInt(args[index]);
        }
        if( (index = getOption("-l", args)) >= 0) {
            l = Double.parseDouble(args[index]);
        }
        if( (index = getOption("-c", args)) >= 0) {
            c = Double.parseDouble(args[index]);
        }
        if( (index = getOption("-suppthreshold", args)) >= 0) {
            suppressionThreshold = Integer.parseInt(args[index]);
        }
        if( (index = getOption("-input", args)) >= 0) {
            inputFilename = args[index];
        }
        if( (index = getOption("-separator", args)) >= 0) {
            separator = args[index];
        }
        if( (index = getOption("-output", args)) >= 0) {
            outputFilename = args[index];
        }
        if( (index = getOption("-method", args)) >= 0) {
            setMethod(args[index]);
        }
    }

    private int getOption(String option, String[] args) {
        int retVal = -1;
        for(int i = 0; i < args.length; i++) {
            if(args[i].equals(option)) {
                retVal = i+1;
                break;
            }
        }
        if(retVal < 0 || retVal >= args.length) {
            return -1;
        } else {
            return retVal;
        }
    }

    public void checkValidity() throws Exception{
        if(inputFilename == null) {
            throw new Exception("Error: No input file");
        }
        File f = new File(inputFilename);
        if(!f.exists()) {
            throw new Exception("Input file not found: " + inputFilename );
        }
        if(outputFilename == null) {
            throw new Exception("Error: No input file specified.");
        }
        f = new File(outputFilename);
        if(f.exists()) {
            System.out.println("WARNING: Output file will be overwritten (" + outputFilename + ")");
        }
    }
}
