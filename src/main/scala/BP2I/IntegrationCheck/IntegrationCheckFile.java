package BP2I.IntegrationCheck;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class IntegrationCheckFile {

    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {

        /**
         * Notes.
         * args formed of three String:
         *      (1) folder where files are
         *      (2) parameter table path (for now)
         *      (3) name of the application
         *
         * Stages:
         *      (0) Check if parameter file is accessible and good.
         *      (1) Check if files already exist in HDFS -- if so, stop the job or just remove these files? (imo stop)
         *      (2) Check if .dat & .des files are both present
         *      (3) Check if types present in .des files are allowed into the datalake
         *
         *      Error code:
         *      (0) => x
         *      (1) => 101
         *      (2) => 110 & 111
         *      (3) => 103
         */

        JavaMiscFunctions mf = new JavaMiscFunctions();

        String reportName = mf.initializeReport(args);

        Path parentDir = new Path(Arrays.asList(args).get(0));

        mf.showStage(0);

        Path parameterPath = new Path(Arrays.asList(args).get(1));

        mf.checkParameter(parameterPath, reportName);

        mf.showStage(1);

        List<Path> listOfPathsStage0 = mf.getFilesPath(parentDir);

        int amountOfFiles = mf.getAmountOfFiles(parentDir);
        System.out.print("INFO: Stage 1, amount of files in parent directory: ");
        System.out.println(amountOfFiles);

        System.out.print("INFO: Stage 1, list of tables to initialize: ");
        List<String> listOfTableNames = mf.getFilesTableName(listOfPathsStage0);
        System.out.println(listOfTableNames);

        System.out.println("WARN: Stage 1, list of files already existing in HDFS, removing them from job: ");
        List<Path> listOfPathsStage1 = mf.filterFilesAlreadyExistingHdfs(parentDir, new JavaParam().hdfsDir, reportName);

        System.out.println("INFO: Stage 1, list of files that are going to stage 2: ");
        for (Path p : listOfPathsStage1) {
            System.out.println(p.toUri());
        }

        mf.showStage(2);

        List<Path> listOfPathsStage2 = mf.filterFilesWithoutDatDes(listOfPathsStage1, reportName);
        System.out.println("INFO: Stage 2, list of files that are going to stage 3: ");
        for (Path p : listOfPathsStage2) {
            System.out.println(p.toUri());
        }

        mf.showStage(3);

        List<Path> finalPath = mf.filterFilesWithoutAllowedTypes(listOfPathsStage2, reportName);

        System.out.println("INFO: Stage 3, list of files that are going to stage 4: ");
        for (Path p : finalPath) {
            System.out.println(p.toUri());
        }



    }
}