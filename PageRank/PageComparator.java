
/**
 * Comparator overrides the default comparator
 * and sort the values in descending order
 * 
 * 
 * 
 * @author Ajaykumar Prathap
 * @since 2017-03-20
 */


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class PageComparator extends WritableComparator {

    protected PageComparator(){
        super(DoubleWritable.class, true);
    }
    //override the compare method to sort the rank in descending order
    @Override
    public int compare(WritableComparable first, WritableComparable second) {
        DoubleWritable firstValue = (DoubleWritable) first;
        DoubleWritable secondValue = (DoubleWritable) second;
        return -1 * firstValue.compareTo(secondValue);
    }
}
