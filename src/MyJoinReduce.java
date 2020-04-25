import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Vector;

public class MyJoinReduce extends Reducer<Text,Text,Text, NullWritable> {

    private String emptyString="null";
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
    {
        //key: product ID
        //value: product name, product price
        //value: order ID, order date, quantity
        //left join:order as left
        //output: id, date, pid, name, price, num
        //output: order ID,order date, product ID, product name, product price, quantity

        //TODO:keep it empty or value as NULL?

        Vector<String> order=new Vector<String>();
        Vector<String> product=new Vector<String>();
        for(Text t:values)
        {
            if(t.toString().split(" ").length==3)
                order.add(t.toString());
            else
                product.add(t.toString());
        }

        if(product.isEmpty())
        {
            //left join situation
            for (String s : order) {
                String[] o = s.split(" ");
                //String[] p=product.get(i).split(" ");
                String keyout = o[0] + " " + o[1] + " " + key.toString() + " " + emptyString + " " + emptyString + " " + o[2];
                context.write(new Text(keyout), NullWritable.get());
            }
        }
        else
        {
            for (String value : order) {
                for (String s : product) {
                    String[] o = value.split(" ");
                    String[] p = s.split(" ");
                    String keyout = o[0] + " " + o[1] + " " + key.toString() + " " + p[0] + " " + p[1] + " " + o[2];
                    context.write(new Text(keyout), NullWritable.get());
                }
            }

        }

    }
}
