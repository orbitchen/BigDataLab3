import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyJoinMap extends Mapper<Object,Text, Text,Text> {

    @Override
    public void map(Object key,Text value,Context context)
        throws IOException,InterruptedException
    {
        FileSplit fs=(FileSplit)context.getInputSplit();
        String fileName=fs.getPath().getName().split("\\.")[0];
        String keyout,valout;
        String[] s=value.toString().split(" ");
        if(fileName.equals("order"))
        {
            //order
            //order ID, order date, product ID, quantity
            keyout=s[2];
            valout=s[0]+" "+s[1]+" "+s[3];
        }
        else
        {
            //product
            //product ID, name, price
            keyout=s[0];
            valout=s[1]+" "+s[2];
        }
        context.write(new Text(keyout),new Text(valout));

    }
}
