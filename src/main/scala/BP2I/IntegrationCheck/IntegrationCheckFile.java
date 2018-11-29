package BP2I.IntegrationCheck;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class IntegrationCheckFile {

    public static void main(String[] args) throws IOException {

        JavaMiscFunctions mf = new JavaMiscFunctions();

        mf.initializeReport("./file-name.txt");

        Path parentDir = new Path(Arrays.asList(args).get(0));

        mf.showStage(0);

        Path parameterPath = new Path(Arrays.asList(args).get(1));

        mf.checkParameter(parameterPath);

        mf.showStage(1);

        List<Path> listOfPathsStage0 = mf.getFilesPath(parentDir);

        int amountOfFiles = mf.getAmountOfFiles(parentDir);
        System.out.print("INFO: Stage 1, amount of files in parent directory: ");
        System.out.println(amountOfFiles);

        System.out.print("INFO: Stage 1, list of tables to initialize: ");
        List<String> listOfTableNames = mf.getFilesTableName(listOfPathsStage0);
        System.out.println(listOfTableNames);

        System.out.println("WARN: Stage 1, list of files already existing in HDFS, removing them from job: ");
        List<Path> listOfPathsStage1 = mf.filterFilesAlreadyExistingHdfs(parentDir, new JavaParam().hdfsDir);

        System.out.println("INFO: Stage 1, list of files that are going to stage 2: ");
        for (Path p : listOfPathsStage1) {
            System.out.println(p.toUri());
        }

        mf.showStage(2);

        List<Path> listOfPathsStage2 = mf.filterFilesWithoutDatDes(listOfPathsStage1);
        System.out.println("INFO: Stage 2, list of files that are going to stage 3: ");
        for (Path p : listOfPathsStage2) {
            System.out.println(p.toUri());
        }

        mf.showStage(3);

        List<Path> finalPath = mf.filterFilesWithoutAllowedTypes(listOfPathsStage2);

        System.out.println("INFO: Stage 3, list of files that are going to stage 4: ");
        for (Path p : finalPath) {
            System.out.println(p.toUri());
        }
    }
}