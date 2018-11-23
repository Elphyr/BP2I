package BP2I.IntegrationCheck;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class IntegrationCheckFile {

    public static void main(String[] args) throws IOException, SQLException {

        JavaMiscFunctions mf = new JavaMiscFunctions();

        Path parentDir = new Path("./JavaTest");

        List<Path> listOfPathTotal = mf.getFilesPath(parentDir);

        System.out.print("List of tables in parent directory: ");
        List<String> listOfTableNames = mf.getFilesTableName(listOfPathTotal);
        System.out.println(listOfTableNames);

        int amountOfFiles = mf.getAmountOfFiles(parentDir);
        System.out.print("Amount of files in parent directory: ");
        System.out.println(amountOfFiles);

        Path goodDir = new Path("./JavaTest/bool-tab");

        Path badDir = new Path("./JavaTest/ca-resource-class-error");

        System.out.println("======================== GOOD DIRECTORY ========================");

        List<Path> listOfPathGood = mf.getFilesPath(goodDir);

        System.out.println(listOfPathGood);

        System.out.print("Does .des file exist? ");
        System.out.println(mf.checkDesExists(listOfPathGood));
        System.out.print("Does .dat file exist? ");
        System.out.println(mf.checkDatExists(listOfPathGood));

        Path goodDesPath = mf.getDesFilePath(listOfPathGood);

        List<String> abcd = mf.getColumnsFromDesFile(goodDesPath.toUri().getRawPath());

        System.out.println(abcd);


        System.out.println("======================== BAD DIRECTORY ========================");

        List<Path> listOfPathBad = mf.getFilesPath(badDir);

        System.out.println(listOfPathBad);

        System.out.print("Does .des file exist? ");
        System.out.println(mf.checkDesExists(listOfPathBad));
        System.out.print("Does .dat file exist? ");
        System.out.println(mf.checkDatExists(listOfPathBad));

        System.out.println("======================== PARAMETERS TABLE ========================");

        String parameterPath = "/home/raphael/Documents/Lincoln/2018-BP2I/reftec_2018/tables_de_parametrage/parametre_application_draft1";

        List<String> toto = mf.getListOfTablesFromParameter(parameterPath);
        
        System.out.println(toto);


        System.out.println("========================");

    }
}