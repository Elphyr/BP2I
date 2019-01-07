package BP2I.IntegrationServeurCollecte.Utils;

import BP2I.IntegrationServeurCollecte.Func.MiscFunctions;
import BP2I.IntegrationServeurCollecte.Func.StageFunctions;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class IntegrationChecks {

    /**
     * Notes.
     * args formed of a single String:
     * - the path to the folder to watch.
     * Stages:
     * (0) Check if parameter file is accessible and good.
     * (1) Check if files have the right names.
     * (2) Check if .dat & .des files are both present
     * (3) Check if files already exist in HDFS -- if so, stop the job or just remove these files? (imo stop)
     * (4) Check if types present in .des files are allowed into the datalake
     * Error code:
     * (0) => x
     * (1) => x
     * (2) => 101
     * (3) => 110 & 111
     * (4) => 103
     */
    public static void integrationChecksSingleFolder(Path dirPath, Path parameterPath, String appName) throws SQLException, IOException, ClassNotFoundException {

        String[] args = new String[]{dirPath.toString(), parameterPath.toString(), appName};

        String reportName = MiscFunctions.initializeReport(args);

        StageFunctions.showStage(0);

        StageFunctions.checkTypesInParameter(parameterPath, reportName);

        StageFunctions.showStage(1);

        List<Path> listOfPathsStage0 = MiscFunctions.getFilesPath(dirPath);

        List<Path> listOfPathsStage1 = StageFunctions.filerFilesWrongName(listOfPathsStage0, new Path(IntegrationProperties.appParamDir), reportName);

        System.out.println("INFO: Stage 1, list of files that are going to stage 2: ");
        for (Path p : listOfPathsStage1) {
            System.out.println(p.toUri());
        }

        StageFunctions.showStage(2);

        List<Path> listOfPathsStage2 = StageFunctions.filterFilesWithoutDatDes(listOfPathsStage1, reportName);
        System.out.println("INFO: Stage 2, list of files that are going to stage 3: ");
        for (Path p : listOfPathsStage2) {
            System.out.println(p.toUri());
        }

        StageFunctions.showStage(3);

        List<Path> listOfPathsStage3 = StageFunctions.filterFilesAlreadyExistingHdfs(listOfPathsStage2, new Path(IntegrationProperties.hdfsDir), reportName);

        System.out.println("INFO: Stage 3, list of files that are going to stage 4: ");
        for (Path p : listOfPathsStage3) {
            System.out.println(p.toUri());
        }

        StageFunctions.showStage(4);

        List<Path> listOfPathsStage4 = StageFunctions.filterFilesWithoutAllowedTypes(listOfPathsStage3, reportName);

        System.out.println("INFO: Stage 4, list of files that are going to stage 5: ");
        for (Path p : listOfPathsStage4) {
            System.out.println(p.toUri());
        }

        MiscFunctions.moveToGood(listOfPathsStage4, IntegrationProperties.goodDir);

        MiscFunctions.moveToBad(listOfPathsStage0, listOfPathsStage4, IntegrationProperties.badDir);
    }
}