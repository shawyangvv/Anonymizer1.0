package anonymizer;

public class Interval {
    public double low;
    public double high;
    private String content;
    public int incType;

    public static final int TYPE_IncLowIncHigh = 1;
    public static final int TYPE_IncLowExcHigh = 2;
    public static final int TYPE_ExcLowIncHigh = 3;
    public static final int TYPE_ExcLowExcHigh = 4;

    public Interval(String stringRep) throws Exception{
        if((stringRep.startsWith("[") || stringRep.startsWith("("))
                && (stringRep.endsWith("]") || stringRep.endsWith(")"))) {
            if(stringRep.contains(":")) {
                String[] boundaries = stringRep.substring(1, stringRep.length()-1).split(":");
                low = Double.parseDouble(boundaries[0]);
                high = Double.parseDouble(boundaries[1]);

                boolean incLow = false;
                if(stringRep.charAt(0) == '[') {
                    incLow = true;
                }
                boolean incHigh = false;
                if(stringRep.charAt(stringRep.length()-1) == ']') {
                    incHigh = true;
                }
                if(incLow) {
                    if(incHigh) {
                        incType = TYPE_IncLowIncHigh;
                    } else {
                        incType = TYPE_IncLowExcHigh;
                    }
                } else {
                    if(incHigh) {
                        incType = TYPE_ExcLowIncHigh;
                    } else {
                        incType = TYPE_ExcLowExcHigh;
                    }
                }
                content = stringRep;
            } else {
                if(stringRep.startsWith("(") && stringRep.endsWith(")")) {
                    throw new Exception("Empty interval (" + content + ")!");
                }
                stringRep = stringRep.substring(1, stringRep.length()-1);
                double val = Double.parseDouble(stringRep);
                low = high = val;
                content = "[" + Double.toString(val) + "]";
                incType = TYPE_IncLowIncHigh;
            }
        } else {
            double val = Double.parseDouble(stringRep);
            low = high = val;
            content = "[" + Double.toString(val) + "]";
            incType = TYPE_IncLowIncHigh;
        }

        if(low > high || (low == high && incType == TYPE_ExcLowExcHigh)) {
            throw new Exception("Empty interval (" + content + ")!");
        }
    }

    public boolean belongTo(String val){
        try {
            double d = Double.parseDouble(val);
            return belongTo(d);
        } catch(Exception e) {
            try {
                Interval i = new Interval(val);
                return belongTo(i.low);
            } catch(Exception e1) {
                e1.printStackTrace();
            }
        }
        return false;
    }

    public boolean belongTo(double d) {
        switch (incType) {
            case TYPE_ExcLowExcHigh:
                if(low < d && d < high)
                    return true;
                else
                    return false;
            case TYPE_ExcLowIncHigh:
                if(low < d && d <= high)
                    return true;
                else
                    return false;
            case TYPE_IncLowExcHigh:
                if(low <= d && d < high)
                    return true;
                else
                    return false;
            case TYPE_IncLowIncHigh:
                if(low <= d && d <= high)
                    return true;
                else
                    return false;
            default:
                return true;
        }
    }

    private boolean incLowerBound() {
        if (incType==TYPE_IncLowExcHigh || incType==TYPE_IncLowIncHigh){
            return true;
        } else {
            return false;
        }
    }

    private boolean incUpperBound() {
        if (incType==TYPE_IncLowIncHigh || incType==TYPE_ExcLowIncHigh){
            return true;
        } else{
            return false;
        }
    }

    public boolean contains(Interval val) {
        if(this.low > val.low)
            return false;
        if(this.high < val.high)
            return false;
        if(this.low == val.low && val.incLowerBound() && !this.incLowerBound()) {
            return false;
        }
        if(this.high == val.high && val.incUpperBound() && !this.incUpperBound()) {
            return false;
        }
        return true;
    }

    public boolean singleton() {
        if(!content.contains(":")) {
            return true;
        }
        return false;
    }

    public Interval[] splitInterval(double value) throws Exception{
        Interval[] retVal = new Interval[2];
        if(incLowerBound()) {
            retVal[0] = new Interval("[" + low + ":" + value + "]");
        } else {
            retVal[0] = new Interval("(" + low + ":" + value + "]");
        }
        if(incUpperBound()) {
            retVal[1] = new Interval("(" + value + ":" + high + "]");
        } else {
            retVal[1] = new Interval("(" + value + ":" + high + ")");
        }
        return retVal;
    }

    public String checkInDB(String comparator) {
        String retVal = "";
        if(incLowerBound()) {
            retVal += low + "<=" + comparator;
        } else {
            retVal += low + "<" + comparator;
        }
        retVal += " AND ";
        if(incUpperBound()) {
            retVal += comparator + "<=" + high;
        } else {
            retVal += comparator + "<" + high;
        }
        return retVal;
    }

    public String toString() {
        return content;
    }
}
