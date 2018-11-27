package BP2I.IntegrationCheck;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public class IntegrationCheckFile {

    public static void main(String[] args) throws IOException {

        JavaMiscFunctions mf = new JavaMiscFunctions();

        System.out.println("======================== ############## ========================");
        System.out.println("=========================== STAGE 0 ============================");
        System.out.println("Initialize files to upload into HDFS.");
        System.out.println("======================== ############## ========================");
        System.out.println();

        Path parentDir = new Path("./JavaTest");
        Path hdfsDir = new Path("/home/raphael/Documents/Lincoln/BP2I/Simulation_hdfs");

        List<Path> listOfPathsStage0 = mf.getFilesPath(parentDir);

        int amountOfFiles = mf.getAmountOfFiles(parentDir);
        System.out.print("INFO: Stage 0, amount of files in parent directory: ");
        System.out.println(amountOfFiles);

        System.out.print("INFO: Stage 0, list of tables to initialize: ");
        List<String> listOfTableNames = mf.getFilesTableName(listOfPathsStage0);
        System.out.println(listOfTableNames);

        System.out.println("WARN: Stage 0, list of files already existing in HDFS, removing them from job: ");
        List<Path> listOfPathsStage1 = mf.filterFilesAlreadyExistingHdfs(parentDir, hdfsDir);

        System.out.println("INFO: Stage 0, list of files that are going to stage 1: ");
        for (Path p : listOfPathsStage1) {
            System.out.println(p.toUri());
        }

        System.out.println();
        System.out.println("======================== ############## ========================");
        System.out.println("=========================== STAGE 1 ============================");
        System.out.println("Check if all files are here (.dat & .des).");
        System.out.println("======================== ############## ========================");
        System.out.println();

        List<Path> listOfPathsStage2 = mf.filterFilesWithoutDatDes(listOfPathsStage1);
        System.out.println("INFO: Stage 1, list of files that are going to stage 2: ");
        for (Path p : listOfPathsStage2) {
            System.out.println(p.toUri());
        }


        System.out.println("======================== ############## ========================");
        System.out.println();



        Path parameterPath = new Path("/home/raphael/Documents/Lincoln/BP2I/Tables_parametrage/parametre_application_draft1");

        List<String> listOfTablesFromParameter = mf.getListOfTablesFromParameter(parameterPath);

        System.out.println(listOfTablesFromParameter);
    }
}