package fop.w10join;


import java.util.Arrays;

public class Customer {
    int custKey;
    String mktsegment;
    private char[] varChar;
    private int nationkey;
    private char[] phone;
    private float acctbal;
    private char[] comment;

    public Customer(int custKey, char[] varChar, int nationkey, char[] phone, float acctbal, String mktsegment,
                    char[] comment) {
        this.custKey = custKey;
        this.varChar = varChar;
        this.nationkey = nationkey;
        this.phone = phone;
        this.acctbal = acctbal;
        this.mktsegment = mktsegment;
        this.comment = comment;
    }

    public int custKey() {
        return custKey;
    }

    public String mktsegment() {
        return mktsegment;
    }

    public void output() {
        System.out.println(this.toString());
    }

    @Override
    public String toString() {
        return "Customer{" +
                "custKey=" + custKey +
                ", varChar=" + Arrays.toString(varChar) +
                ", nationkey=" + nationkey +
                ", phone=" + Arrays.toString(phone) +
                ", acctbal=" + acctbal +
                ", mktsegment=" + mktsegment +
                ", comment=" + Arrays.toString(comment) +
                '}';
    }
}
