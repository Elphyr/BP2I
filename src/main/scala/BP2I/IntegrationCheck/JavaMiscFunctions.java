package BP2I.IntegrationCheck;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.spark_project.guava.base.Predicates;
import org.spark_project.guava.collect.Collections2;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class JavaMiscFunctions {

    /**
     * STAGE 0
     * @param paramPath
     * @param reportName
     * @throws IOException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    void checkParameter(Path paramPath, String reportName) throws IOException, SQLException, ClassNotFoundException {

        List<String> listOfAcceptedTypes = new JavaParam().acceptedTypes;

        List<String> types = getTypesFromParameter(paramPath.toUri().getRawPath());

        List<String> listOfRefusedTypes = new ArrayList<>();

        for (String type : types) {

            if (!listOfAcceptedTypes.contains(type)) {

                listOfRefusedTypes.add(type);
            }
        }

        if (listOfRefusedTypes.isEmpty()) {

            System.out.println("Types are fine in the parameter file.");
            String line = new JavaParam().dateFormatForInside.format(new JavaParam().date).concat(";0;OK;");
            writeInReport(reportName, line);
            new JDBCFunctions().writeStageResultIntoTable(reportName, new JavaParam().dateFormatForInside.format(new JavaParam().date), "0", "OK", "", "");

        } else {

            System.out.println("Amount of types to change: " + listOfRefusedTypes.size());
            System.out.println("Types to change: " + listOfRefusedTypes);
            System.out.println("The parameter table is wrong: please correct type before going further.");

            String commentary = "Types to change: " + listOfRefusedTypes;

            String line = new JavaParam().dateFormatForInside.format(new JavaParam().date).concat(";0;KO;100");
            writeInReport(reportName, line);
            new JDBCFunctions().writeStageResultIntoTable(reportName, new JavaParam().dateFormatForInside.format(new JavaParam().date), "0", "KO", "100", commentary);

        }
    }

    /**
     * STAGE 1
     * @param parentPath
     * @param hdfsPath
     * @return
     * @throws IOException
     */
    List<Path> filterFilesAlreadyExistingHdfs(Path parentPath, Path hdfsPath, String reportName) throws IOException, SQLException, ClassNotFoundException {

        List<Path> listOfFilesToIntegrate = getFilesPath(parentPath);

        List<Path> listOfPathStage1 = new ArrayList<>();

        List<String> listOfFileNamesHDFS = getFilesPath(hdfsPath).stream().map(Path::getName).collect(Collectors.toList());

        for (Path p : listOfFilesToIntegrate) {

            if (listOfFileNamesHDFS.contains(p.getName())) {

                System.out.println("WARN: " + p.getName() + " ALREADY EXISTS IN THE DATALAKE!");
            } else {

                listOfPathStage1.add(p);
            }
        }

        if (listOfFileNamesHDFS.isEmpty()) {

            System.out.println("All files already exist in the datalake.");

            String line = new JavaParam().dateFormatForInside.format(new JavaParam().date).concat(";1;KO;101");

            new JDBCFunctions().writeStageResultIntoTable(reportName, new JavaParam().dateFormatForInside.format(new JavaParam().date), "1", "KO", "101", "");
            writeInReport(reportName, line);

        } else {

            String line = new JavaParam().dateFormatForInside.format(new JavaParam().date).concat(";1;OK;");
            new JDBCFunctions().writeStageResultIntoTable(reportName, new JavaParam().dateFormatForInside.format(new JavaParam().date), "1", "OK", "", "");

            writeInReport(reportName, line);
        }

        return listOfPathStage1;
    }

    /**
     * STAGE 2
     * @param listOfPath
     * @param reportName
     * @return
     * @throws IOException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    List<Path> filterFilesWithoutDatDes(List<Path> listOfPath, String reportName) throws IOException, SQLException, ClassNotFoundException {

        List<Path> listOfPathStage2 = new ArrayList<>();

        List<String> flag = new ArrayList<>();
        List<String> commentary = new ArrayList<>();

        for (Path path : listOfPath) {

            List<String> files = getFilesPath(path.getParent()).stream().map(Path::toString).collect(Collectors.toList());

            Boolean condDat = !Collections2.filter(files, Predicates.containsPattern(".dat")).isEmpty();
            Boolean condDes = !Collections2.filter(files, Predicates.containsPattern(".des")).isEmpty();

            if (condDat && condDes) {

                System.out.println(path.getName() + " is good!");
                listOfPathStage2.add(path);

            } else if (condDat) {

                System.out.println(path.getName() + " lack its buddy .des file!");
                commentary.add(path.getName() + " lack its buddy .des file!" );
                flag.add("110");

            } else if (condDes) {

                System.out.println(path.getName() + " lack its buddy .dat file!");
                commentary.add(path.getName() + " lack its buddy .dat file!" );
                flag.add("111");

            } else {

                System.out.println("No description and data file found!");
                flag.add("112");
            }
        }

        if (flag.isEmpty()) {

            new JDBCFunctions().writeStageResultIntoTable(reportName, new JavaParam().dateFormatForInside.format(new JavaParam().date), "2", "OK", "", "");
            String line = new JavaParam().dateFormatForInside.format(new JavaParam().date).concat(";2;OK;");
            writeInReport(reportName, line);
        } else {

            new JDBCFunctions().writeStageResultIntoTable(reportName, new JavaParam().dateFormatForInside.format(new JavaParam().date), "2", "KO", flag.get(0), commentary.toString());
            String line = new JavaParam().dateFormatForInside.format(new JavaParam().date).concat(";2;KO;" + flag.get(0));
            writeInReport(reportName, line);

        }

        return listOfPathStage2;
    }

    /**
     * STAGE 3
     * @param listOfPaths
     * @param reportName
     * @return
     * @throws IOException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    List<Path> filterFilesWithoutAllowedTypes(List<Path> listOfPaths, String reportName) throws IOException, SQLException, ClassNotFoundException {

        List<String> listOfAcceptedTypes = new JavaParam().acceptedTypes;

        List<String> listOfFilesToRemove = new ArrayList<>();

        List<Path> listOfPathStage3 = new ArrayList<>();

        for (Path path : listOfPaths) {

            Path desPath = getDesFilePath(getFilesPath(path.getParent()));

            List<String> listOfTypes = getTypesFromDesFile(desPath.toUri().getRawPath());

            for (String type : listOfTypes) {

                if (!listOfAcceptedTypes.contains(type)) {

                    listOfFilesToRemove.add(path.getName());
                }
            }

            if (!listOfFilesToRemove.contains(path.getName())) {

                listOfPathStage3.add(path);
            }
        }

        if (listOfFilesToRemove.isEmpty()) {

            System.out.println("Types are fine in all tables.");
            String line = new JavaParam().dateFormatForInside.format(new JavaParam().date).concat(";0;OK;");
            writeInReport(reportName, line);
            new JDBCFunctions().writeStageResultIntoTable(reportName, new JavaParam().dateFormatForInside.format(new JavaParam().date), "3", "OK", "", "");

        } else {

            System.out.println("Amount of types to change: " + listOfFilesToRemove.size());
            System.out.println("Types to change: " + listOfFilesToRemove);

            String line = new JavaParam().dateFormatForInside.format(new JavaParam().date).concat(";3;KO;100");
            writeInReport(reportName, line);
            new JDBCFunctions().writeStageResultIntoTable(reportName, new JavaParam().dateFormatForInside.format(new JavaParam().date), "3", "KO", "100", "");

        }

        return listOfPathStage3;
    }

    void showStage(int stageNbr) {

        String stageDes = "";
        if      (stageNbr == 0) stageDes = "Check if types written in parameter table are usable in the datalake.";
        else if (stageNbr == 1) stageDes = "Check if any file already exists in the datalake.";
        else if (stageNbr == 2) stageDes = "Check if all files are here (.dat & .des).";
        else if (stageNbr == 3) stageDes = "Check if all types in the .des file are accepted in the datalake.";

        System.out.println("\n" +
                "======================== ############## ========================" + "\n" +
                "=========================== STAGE " + stageNbr + " ============================" + "\n" +
                stageDes + "\n" +
                "======================== ############## ========================" + "\n");
    }

    List<Path> getFilesPath(Path path) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        RemoteIterator<LocatedFileStatus> directories = fs.listFiles(path, true);

        List<Path> listOfPath = new ArrayList<>();

        while (directories.hasNext()) {
            LocatedFileStatus fileStatus = directories.next();

            listOfPath.add(fileStatus.getPath());
        }

        return listOfPath;
    }

    List<String> getFilesTableName(List<Path> listOfPath) {

        List<String> listOfTables = new ArrayList<>();

        for (Path path : listOfPath) {

            List<String> separatedFilesPath = Lists.reverse(
                    Arrays.asList(
                            path.toUri().getRawPath()
                                    .replace("[", "").replace("]", "")
                                    .split("/")));

            List<String> separatedFileName = Arrays.asList(separatedFilesPath.get(0).split("_"));

            try {

                listOfTables.add(separatedFileName.get(1));

            }
            catch (ArrayIndexOutOfBoundsException e) {

                System.out.println("There is a problem here: " + path.toUri().getRawPath());
            }
        }

        return listOfTables.stream().distinct().collect(Collectors.toList());
    }

    int getAmountOfFiles(Path dirPath) throws IOException {

        List<Path> listOfFilesPath = getFilesPath(dirPath);

        List<String> listOfFilesNames = new ArrayList<>();

        for (Path path : listOfFilesPath) {

            List<String> separatedFilesPath = Lists.reverse(
                    Arrays.asList(
                            path.toUri().getRawPath()
                                    .replace("[", "").replace("]", "")
                                    .split("/")));

            listOfFilesNames.add(separatedFilesPath.get(0));
        }

        List<String> filteredListOfFilesNames = listOfFilesNames.stream()
                .filter(x -> x.contains(".dat") || x.contains(".des")).collect(Collectors.toList());

        return filteredListOfFilesNames.size();
    }

    private Path getDesFilePath(List<Path> listOfPaths) {

        List<String> listOfFiles = new ArrayList<>();

        for (Path path : listOfPaths) {

            listOfFiles.add(path.toString());
        }

        Path desPath = new Path(Collections2.filter(listOfFiles, Predicates.containsPattern(".des")).toString().replace("[", "").replace("]", ""));

        return desPath;
    }

    private List<String> getTypesFromDesFile(String fileAbsolutePath) throws IOException {

        File file = new File(fileAbsolutePath);

        List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);

        List<String> listOfColumns = new ArrayList<>();

        for (String line : lines) {

            String[] array = line.split(";", -1);

            listOfColumns.add(array[2]);
        }

        listOfColumns.remove(0);

        return listOfColumns.stream().distinct().collect(Collectors.toList());
    }

    List<String> getTablesFromParameter(Path fileAbsolutePath) throws IOException {

        File file = new File(fileAbsolutePath.toUri().getRawPath());

        List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);

        List<String> listOfColumns = new ArrayList<>();

        for (String line : lines) {

            String[] array = line.split(";", -1);

            listOfColumns.add(array[1]);
        }

        listOfColumns.remove(0);

        return listOfColumns.stream().distinct().collect(Collectors.toList());
    }

    private List<String> getTypesFromParameter(String paramPath) throws IOException {

        File file = new File(paramPath);

        List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);

        List<String> listOfTypes = new ArrayList<>();

        for (String line : lines) {

            String[] array = line.split(";", -1);

            listOfTypes.add(array[3]);
        }

        listOfTypes.remove(0);

        return listOfTypes.stream().distinct().collect(Collectors.toList());
    }

    String initializeReport(String[] args) throws IOException, SQLException, ClassNotFoundException {

        String reportName = Arrays.asList(args).get(2) + "_" + new JavaParam().dateFormatForOutside.format(new JavaParam().date);

        java.nio.file.Path file = Paths.get(reportName);

        Files.deleteIfExists(file);

        List<String> logColumns = Collections.singletonList("date;stage;result;error_code");
        Files.write(file, logColumns, Charset.forName("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        new JDBCFunctions().dropTable(reportName);

        return reportName;
    }

    private void writeInReport(String reportName, String line) throws IOException {

        java.nio.file.Path file = Paths.get(reportName);

        List<String> lines = Collections.singletonList(line);

        Files.write(file, lines, Charset.forName("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    void moveToGood(List<Path> goodPaths, String goodDir) throws IOException {

        for (Path p : goodPaths) {

            String tableName = p.getParent().getName();

            FileUtils.copyDirectory(new File(p.getParent().toUri().getRawPath()), new File(goodDir + "/" + tableName));
        }
    }

    void moveToBad(List<Path> badPaths, List<Path> goodPaths, String badDir) throws IOException {

        for (Path p : badPaths) {

            if (!goodPaths.contains(p)) {

                String tableName = p.getParent().getName();

                FileUtils.copyDirectory(new File(p.getParent().toUri().getRawPath()), new File(badDir + "/" + tableName));
            }
        }
    }
}