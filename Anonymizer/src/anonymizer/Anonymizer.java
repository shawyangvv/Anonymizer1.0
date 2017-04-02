package anonymizer;

import datafly.Datafly;
import incognito.Incognito_L;
import incognito.Incognito_T;
import mondrian.Mondrian;
import incognito.Incognito_K;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

import java.util.LinkedList;
import java.util.ListIterator;

import java.sql.ResultSet;
import databasewrapper.SqLiteWrapper;
import databasewrapper.QueryResult;
import databasewrapper.DatabaseWrapper;


public abstract class Anonymizer {
    protected DatabaseWrapper databaseWrapper;

    protected Configuration conf;
    protected int suppressionThreshold;

    protected EquivalenceTable eqTable;
    protected AnonRecordTable anonTable;

    public Anonymizer(Configuration conf){
        this.conf = conf;
    }

    protected abstract EquivalenceTable createEquivalenceTable(String tableName);
    protected abstract AnonRecordTable createAnonRecordsTable(String tableName);

    protected abstract long insertTupleToEquivalenceTable(String[] vals) throws Exception;
    protected abstract void insertTupleToAnonTable(String[] vals, long eid) throws Exception;

    public abstract void anonymize() throws Exception;

    public void readData() throws Exception{
        String fileName = conf.inputFilename;
        FileReader fr = new FileReader(fileName);
        BufferedReader input = new BufferedReader(fr);

        String line = input.readLine();
        int count=0;

        while( (line = input.readLine()) != null && line.length() > 0) {
            if(line.contains("?")) {
                continue;
            }
            count++;
            String[] vals = line.split(conf.separator);
            for(int i = 0; i < vals.length; i++) {
                vals[i] = vals[i].trim();
            }
            Long eid = insertTupleToEquivalenceTable(vals);
            insertTupleToAnonTable(vals, eid);
        }

        System.out.println("read in "+ count +" records");
        input.close();
    }

    public void outputResults() throws Exception{
        FileWriter fw = new FileWriter(conf.outputFilename);
        BufferedWriter output = new BufferedWriter(fw);

        FileReader fr = new FileReader(conf.inputFilename);
        BufferedReader input = new BufferedReader(fr);

        String writeLine = System.getProperty("line.separator");
        String header;
        boolean flag = true;
        while ((flag)&&(header=input.readLine())!=null && header.length()>0) {
            String[] title=header.split(conf.separator);
            header="";
            for (int i = 0; i < title.length; i++){
                if(title[i] != null) {
                    header += title[i] + conf.separator;
                }
            }
            header += "\n";
            output.write(header);
            flag = false;
        }

        long rid = 1;
        String inputline;
        String line;
        while( (inputline = input.readLine()) != null && inputline.length() > 0) {
            if(inputline.contains("?")) {
                continue;
            }
            String[] vals = inputline.split(conf.separator);
            String getEIDforRID  = "SELECT EID FROM " + anonTable.getName() + " WHERE RID = " + rid;
            QueryResult result = databaseWrapper.executeQuery(getEIDforRID);
            Long eid = ((ResultSet) result.next()).getLong(1);
            result.__close();
            rid++;

            String[] genVals = eqTable.getGeneralization(eid);
            String[] orignalCat;
            String connCat="";
            for(int i = 0; i < conf.qidAtts.length; i++) {
                if (conf.qidAtts[i].catMapInt!=null){
                    String orign=vals[conf.qidAtts[i].index].toString();
                    orignalCat=conf.qidAtts[i].mapBack(orign);
                    for (int j = 0; j <orignalCat.length; j++){
                        connCat += orignalCat[j] + ",";
                    }
                    connCat=connCat.substring(0, connCat.length()-1);
                    genVals[i]=connCat;
                } else {
                    vals[conf.qidAtts[i].index] = genVals[i];
                }
            }

            if(conf.kidAtts != null) {
                ListIterator<Integer> iter = conf.kidAtts.listIterator();
                while(iter.hasNext()) {
                    int i = iter.next();
                    long salt = System.currentTimeMillis();
                    String origin = vals[i];
                    Long salted = origin.hashCode() + salt;
                    Integer hashVal = salted.hashCode();
                    hashVal = Math.abs(hashVal);
                    vals[i] = hashVal.toString();
                }
            }

            line = "";
            for(int i = 0; i < vals.length; i++) {
                if(vals[i] != null) {
                    line += conf.separator + vals[i];
                }
            }
            line = line.substring(conf.separator.length());
            line += writeLine;
            output.write(line);
        }
        output.close();
        this.anonTable.drop();
        this.eqTable.drop();
        this.databaseWrapper.flush();
    }

    public LinkedList<Long> checkSuppression(String anonRecordTable) throws Exception{
        int suppressionSize = 0;
        LinkedList<Long> equivalenceForSuppress = new LinkedList<Long>();
        String select_SQL = "SELECT EID, COUNT(*) FROM " + anonRecordTable
                + " GROUP BY EID ORDER BY COUNT(*) ASC";
        QueryResult result = databaseWrapper.executeQuery(select_SQL);
        while(result.hasNext()) {
            ResultSet rs = (ResultSet) result.next();

            Integer currSize = rs.getInt(2);
            if(currSize < conf.k) {
                equivalenceForSuppress.add(rs.getLong(1));
                suppressionSize += currSize;
                if(suppressionThreshold > 0 && suppressionSize > suppressionThreshold) {
                    result.__close();
                    return null;
                }
            } else {
                result.__close();
                return equivalenceForSuppress;
            }
        }
        result.__close();
        return equivalenceForSuppress;
    }

    protected void suppress(LinkedList<Long> suppressionList) {
        String[] genVals = new String[conf.qidAtts.length];
        for(int i = 0; i < genVals.length; i++) {
            genVals[i] = conf.qidAtts[i].supperssionValue();
        }

        if(suppressionList != null) {
            Long suppEID = new Long(-1);
            if(!suppressionList.isEmpty()) {
                try {
                    suppEID = eqTable.insertEquivalence(genVals);
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
            while(!suppressionList.isEmpty()) {
                Long eid = suppressionList.removeFirst();
                eqTable.deleteEquivalence(eid);
                anonTable.updateEID(eid, suppEID);
                System.out.println("Suppressing equivalence " + eid);
            }
        }
    }

    public static void anonymizeDataset(String[] args) throws Exception {
        Configuration conf = new Configuration(args);
        anonymizeDataset(conf);
    }

    public static void anonymizeDataset(Configuration conf) throws Exception {
        SqLiteWrapper.initialize(conf);
        Anonymizer anon = null;
        switch(conf.anonMethod) {
            case Configuration.METHOD_MONDRIAN:
                anon = new Mondrian(conf);
                break;
            case Configuration.METHOD_INCOGNITO_K:
                anon = new Incognito_K(conf);
                break;
            case Configuration.METHOD_INCOGNITO_L:
                anon = new Incognito_L(conf);
                break;
            case Configuration.METHOD_INCOGNITO_T:
                anon = new Incognito_T(conf);
                break;
            case Configuration.METHOD_DATAFLY:
                anon = new Datafly(conf);
                break;
        }

        long start = System.currentTimeMillis();
        anon.readData();
        long stop = System.currentTimeMillis();
        System.out.println("Reading data takes " + (stop - start)/1000.0+ "sec.s");

        start = System.currentTimeMillis();
        anon.anonymize();
        stop = System.currentTimeMillis();
        System.out.println("Anonymization takes " + (stop - start)/1000.0 + "sec.s");

        start = System.currentTimeMillis();
        anon.outputResults();
        stop = System.currentTimeMillis();
        System.out.println("Writing data takes " + (stop - start)/1000.0 + "sec.s");
    }



    public static void main(String[] args) {
        try {
            Anonymizer.anonymizeDataset(args);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
