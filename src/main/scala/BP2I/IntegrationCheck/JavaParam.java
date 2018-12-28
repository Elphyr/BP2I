package BP2I.IntegrationCheck;

import org.apache.hadoop.fs.Path;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class JavaParam {

    /**
     * How it works in datalake for exotic types:
     * types.map { case "nvarchar" => "STRING" ; case "varchar" => "STRING" ; case "char" => "STRING" ; case "nchar" => "STRING" ;
     * case "binary" => "STRING" ; case "varbinary" => "STRING" ; case "timestamp" => "STRING" ; case "datetime" => "STRING" ;
     * case "ntext" => "STRING"; case "image" => "STRING" ; case "money" => "DOUBLE" }
     */
    List<String> acceptedTypes = Arrays.asList("int", "float", "smallint", "nvarchar", "varchar", "char", "nchar",
            "timestamp", "datetime", "ntext", "image", "money");

    Path hdfsDir = new Path("/home/raphael/Documents/Lincoln/BP2I/simulation_hdfs");

    Date date = new Date();
    DateFormat dateFormatForInside = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    DateFormat dateFormatForOutside = new SimpleDateFormat("yyyyMMdd'_'HHmmss");

}
