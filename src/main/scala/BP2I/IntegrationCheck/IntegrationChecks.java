package BP2I.IntegrationCheck;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

class IntegrationChecks {

    private MiscFunctions mf = new MiscFunctions();
    private StageFunctions stg = new StageFunctions();
    private JavaParam jvp = new JavaParam();

    /**
     * Notes.
     * args formed of a single String:
     * - the path to the folder to watch.
     * <p>
     * Stages:
     * (0) Check if parameter file is accessible and good.
     * (1) Check if files already exist in HDFS -- if so, stop the job or just remove these files? (imo stop)
     * (2) Check if .dat & .des files are both present
     * (3) Check if types present in .des files are allowed into the datalake
     * <p>
     * Error code:
     * (0) => x
     * (1) => 101
     * (2) => 110 & 111
     * (3) => 103
     */
    void integrationChecksSingleFolder(Path dirPath, Path parameterPath, String appName) throws SQLException, IOException, ClassNotFoundException {

        String[] args = new String[]{dirPath.toString(), parameterPath.toString(), appName};

        String reportName = mf.initializeReport(args);

        stg.showStage(0);

        stg.checkTypesInParameter(parameterPath, reportName);

        stg.showStage(1);

        List<Path> listOfPathsStage0 = mf.getFilesPath(dirPath);

        System.out.print("INFO: Stage 1, list of tables to initialize: ");
        List<String> listOfTableNames = mf.getFilesTableName(listOfPathsStage0);
        System.out.println(listOfTableNames);

        stg.filerFilesWrongName(listOfPathsStage0, new Path("/home/raphael/Documents/Lincoln/BP2I/parameter_table/application_reftec"), reportName);

        stg.showStage(2);

        int amountOfFiles = mf.getAmountOfFiles(dirPath);
        System.out.print("INFO: Stage 1, amount of files in directory: ");
        System.out.println(amountOfFiles);

        System.out.println("WARN: Stage 1, list of files already existing in HDFS, removing them from job: ");
        List<Path> listOfPathsStage1 = stg.filterFilesAlreadyExistingHdfs(dirPath, jvp.hdfsDir, reportName);

        System.out.println("INFO: Stage 1, list of files that are going to stage 2: ");
        for (Path p : listOfPathsStage1) {
            System.out.println(p.toUri());
        }

        stg.showStage(3);

        List<Path> listOfPathsStage2 = stg.filterFilesWithoutDatDes(listOfPathsStage1, reportName);
        System.out.println("INFO: Stage 2, list of files that are going to stage 3: ");
        for (Path p : listOfPathsStage2) {
            System.out.println(p.toUri());
        }

        stg.showStage(4);

        List<Path> listOfPathsStage3 = stg.filterFilesWithoutAllowedTypes(listOfPathsStage2, reportName);

        System.out.println("INFO: Stage 3, list of files that are going to stage 4: ");
        for (Path p : listOfPathsStage3) {
            System.out.println(p.toUri());
        }

        mf.moveToGood(listOfPathsStage3, "/home/raphael/Documents/Lincoln/BP2I/good");

        mf.moveToBad(listOfPathsStage0, listOfPathsStage3, "/home/raphael/Documents/Lincoln/BP2I/bad");
    }
}