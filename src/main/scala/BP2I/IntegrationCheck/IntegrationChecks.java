package BP2I.IntegrationCheck;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

class IntegrationChecks {

    void integrationChecksSingleFolder(Path dirPath, Path parameterPath, String appName) throws SQLException, IOException, ClassNotFoundException {

        String[] args = new String[] {dirPath.toString(), parameterPath.toString(), appName};

        JavaMiscFunctions mf = new JavaMiscFunctions();

        String reportName = mf.initializeReport(args);

        mf.showStage(0);

        mf.checkParameter(parameterPath, reportName);

        mf.showStage(1);

        List<Path> listOfPathsStage0 = mf.getFilesPath(dirPath);

        int amountOfFiles = mf.getAmountOfFiles(dirPath);
        System.out.print("INFO: Stage 1, amount of files in directory: ");
        System.out.println(amountOfFiles);

        System.out.print("INFO: Stage 1, list of tables to initialize: ");
        List<String> listOfTableNames = mf.getFilesTableName(listOfPathsStage0);
        System.out.println(listOfTableNames);

        System.out.println("WARN: Stage 1, list of files already existing in HDFS, removing them from job: ");
        List<Path> listOfPathsStage1 = mf.filterFilesAlreadyExistingHdfs(dirPath, new JavaParam().hdfsDir, reportName);

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

        List<Path> listOfPathsStage3 = mf.filterFilesWithoutAllowedTypes(listOfPathsStage2, reportName);

        System.out.println("INFO: Stage 3, list of files that are going to stage 4: ");
        for (Path p : listOfPathsStage3) {
            System.out.println(p.toUri());
        }

        mf.moveToGood(listOfPathsStage3, "/home/raphael/Documents/Lincoln/BP2I/good");

        mf.moveToBad(listOfPathsStage0, listOfPathsStage3, "/home/raphael/Documents/Lincoln/BP2I/bad");
    }
}
