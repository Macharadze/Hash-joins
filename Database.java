package fop.w10join;

// TODO Import the stuff you need

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
public class Database {
    private static Stream<Customer> customers;
    private static Stream<LineItem> lineItems;
    private static Stream<Order> orders;

    private static Path baseDataDirectory = Paths.get("data");

    public static void setBaseDataDirectory(Path baseDataDirectory) {
        Database.baseDataDirectory = baseDataDirectory;
    }

public static Function<String,Customer> mapToCustomer=(line)->{
        String[] lines=line.split("\\|");
        String[] str=lines[1].split("#");
        int num= Integer.parseInt(str[1]);
        return new Customer(num,lines[2].toCharArray(),Integer.parseInt(lines[3]),lines[4].toCharArray(),Float.parseFloat(lines[5]),lines[6],lines[7].toCharArray());
};
    public static Function<String,Order> mapToOrder=(line)->{
        String[] lines=line.split("\\|");
        return new Order(Integer.parseInt(lines[0]),Integer.parseInt(lines[1]),lines[2].charAt(0),Float.parseFloat(lines[3])
        ,LocalDate.parse(lines[4]),lines[5].toCharArray(),lines[6].toCharArray(),Integer.parseInt(lines[7]),lines[8].toCharArray());
    };

        public static Function<String,LineItem> mapToLineLtem=(line)->{
        String[] lines=line.split("\\|");
        char ch=lines[9].charAt(0);
        char ch2=lines[9].charAt(0);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/d");
LocalDate localDate=LocalDate.parse(lines[10]);
        LocalDate localDate2=LocalDate.parse(lines[11]);
        LocalDate localDate3=LocalDate.parse(lines[12]);
        return new LineItem(Integer.parseInt(lines[0]),Integer.parseInt(lines[1]),Integer.parseInt(lines[2]),Integer.parseInt(lines[3]),Integer.parseInt(lines[4])*100,Float.parseFloat(lines[5]),
                Float.parseFloat(lines[6]),Float.parseFloat(lines[7]),ch,ch2,localDate,localDate2,localDate3,lines[13].toCharArray(),lines[14].toCharArray(),lines[15].toCharArray());
    };

        public static Stream<Customer> processInputFileCustomer() {
        try {
            Path path=Paths.get("../"+baseDataDirectory.toString(),"customer.tbl");
            customers= Files.lines(path)
                    .map(mapToCustomer)
                    .filter(Objects::nonNull);
            return customers;
        } catch (IOException e) {
            e.getMessage();
        }
            return null;
        }

    public static Stream<LineItem> processInputFileLineItem() {
        // TODO
        try {
            Path path=Paths.get("../"+baseDataDirectory.toString(),"lineitem.tbl");
            lineItems=Files.lines(path)
                    .filter(Objects::nonNull)
                    .map(mapToLineLtem);
            return lineItems;
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        // For quantity of LineItems please use Integer.parseInt(str) * 100.
        return null;
    }

    public static Stream<Order> processInputFileOrders() {
        // TODO
        try {
            Path path=Paths.get("../"+baseDataDirectory.toString(),"orders.tbl");
            orders= Files.lines(path)
                    .filter(Objects::nonNull)
                    .map(mapToOrder);
            return orders;
        } catch (IOException e) {
            System.out.println(""+e.getMessage());
        }
        return null;
    }

    /*
    In order to realize Joins of tables, you should use the classes of
    the interface Map in Java. First, you should map for each Customer,
     custKey -> marketSegment. Then, you should map for each Order, orderkey
      -> marketSegment by using custkey. Now, you should use this second Map
       for iterating over all LineItems in order to determine to which marketSegment it belongs.
     */
        public static long getAverageQuantityPerMarketSegment(String marketsegment) {
            try {
               Map<String,List<Integer>> mapToCustomer;
               mapToCustomer= Objects.requireNonNull(processInputFileCustomer()).filter(i->i.mktsegment.equals(marketsegment)).collect(Collectors.groupingBy(Customer::mktsegment,Collectors.mapping(Customer::custKey,Collectors.toList())));
                Map<String, List<Integer>> mapOfOrder=new HashMap<>();
                mapOfOrder.put(marketsegment, Objects.requireNonNull(processInputFileOrders()).filter(i -> mapToCustomer.get(marketsegment).contains(i.custKey)).map(Order::orderKey).toList());
            return (long) Objects.requireNonNull(processInputFileLineItem()).filter(i->mapOfOrder.get(marketsegment).contains(i.orderKey)).map(LineItem::quantity).mapToLong(i->i).average().getAsDouble();
            }catch (Exception e){
                System.out.println("this kind of market segment does not exist");
            }
return 0;
        }


    public Database() {
        // TODO
        customers=null;
        orders=null;
        lineItems=null;
    }

    public static void main(String[] args){
        // TODO Test your implementation
 }
}
