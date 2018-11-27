package BP2I.IntegrationCheck;

import java.util.Arrays;
import java.util.List;

public class JavaParam {

    /**
     * How it works in datalake for exotic types:
     *     types.map { case "nvarchar" => "STRING" ; case "varchar" => "STRING" ; case "char" => "STRING" ; case "nchar" => "STRING" ;
     *     case "binary" => "STRING" ; case "varbinary" => "STRING" ; case "timestamp" => "STRING" ; case "datetime" => "STRING" ;
     *     case "ntext" => "STRING"; case "image" => "STRING" ; case "money" => "DOUBLE" }
     */
    List<String> acceptedTypes = Arrays.asList("int", "float", "smallint", "nvarchar", "varchar", "char", "nchar",
            "timestamp", "datetime", "ntext", "image", "money");
}
