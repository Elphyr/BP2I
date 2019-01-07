package BP2I.IntegrationServeurCollecte.Utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class IntegrationParams {

    /**
     * How it works in datalake for exotic types:
     * types.map { case "nvarchar" => "STRING" ; case "varchar" => "STRING" ; case "char" => "STRING" ; case "nchar" => "STRING" ;
     * case "binary" => "STRING" ; case "varbinary" => "STRING" ; case "timestamp" => "STRING" ; case "datetime" => "STRING" ;
     * case "ntext" => "STRING"; case "image" => "STRING" ; case "money" => "DOUBLE" }
     */
    public static List<String> acceptedTypes = Arrays.asList("int", "float", "smallint", "nvarchar", "varchar", "char", "nchar",
            "timestamp", "datetime", "ntext", "image", "money");

    public static Date date = new Date();
    public static DateFormat dateFormatForInside = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    public static DateFormat dateFormatForOutside = new SimpleDateFormat("yyyyMMdd'_'HHmmss");

    public static List<String> environments = Arrays.asList("local", "dev", "qualif", "prod");
}
