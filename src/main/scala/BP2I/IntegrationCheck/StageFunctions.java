package BP2I.IntegrationCheck;

import org.apache.hadoop.fs.Path;
import org.spark_project.guava.base.Predicates;
import org.spark_project.guava.collect.Collections2;
import org.spark_project.guava.collect.Lists;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class StageFunctions {

    private MiscFunctions mf = new MiscFunctions();
    private JDBCFunctions jdbc = new JDBCFunctions();
    private JavaParam jvp = new JavaParam();

    /**
     * Show the stage on the screen.
     * @param stageNbr
     */
    void showStage(int stageNbr) {

        String stageDes = "";
             if (stageNbr == 0) stageDes = "Check if types written in parameter table are usable in the datalake.";
        else if (stageNbr == 1) stageDes = "Check if all files have the right name.";
        else if (stageNbr == 2) stageDes = "Check if both .dat & .des files are present.";
        else if (stageNbr == 3) stageDes = "Check if any file already exists in the datalake.";
        else if (stageNbr == 4) stageDes = "Check if all types in the .des file are accepted in the datalake.";

        System.out.println("\n" +
                "======================== ############## ========================" + "\n" +
                "=========================== STAGE " + stageNbr + " ============================" + "\n" +
                stageDes + "\n" +
                "======================== ############## ========================" + "\n");
    }

    /**
     * STAGE 0
     * Check if the parameter file is readable and has right types.
     * @param tableParamPath
     * @param reportName
     * @throws IOException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    void checkTypesInParameter(Path tableParamPath, String reportName) throws IOException, SQLException, ClassNotFoundException {

        List<String> typesFromParameter = mf.getTypesFromParameter(tableParamPath.toUri().getRawPath());

        List<String> listOfRefusedTypes = new ArrayList<>();

        for (String type : typesFromParameter) {

            if (!jvp.acceptedTypes.contains(type)) {

                listOfRefusedTypes.add(type);
            }
        }

        if (listOfRefusedTypes.isEmpty()) {

            System.out.println("Types are fine in the parameter file.");
            String line = jvp.dateFormatForInside.format(jvp.date).concat(";0;OK;");
            mf.writeInReport(reportName, line);
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "0", "OK", "", "");

        } else {

            System.out.println("Amount of types to change: " + listOfRefusedTypes.size());
            System.out.println("Types to change: " + listOfRefusedTypes);
            System.out.println("The parameter table is wrong: please correct type before going further.");

            String commentary = "Types to change: " + listOfRefusedTypes;

            String line = jvp.dateFormatForInside.format(jvp.date).concat(";0;KO;100");
            mf.writeInReport(reportName, line);
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "0", "KO", "100", commentary);
        }
    }

    /**
     * Stage 1
     * Check if the name of the files follows the right rules.
     * @param listOfPath
     * @param appParamPath
     * @param reportName
     * @return
     * @throws IOException
     */
    List<Path> filerFilesWrongName(List<Path> listOfPath, Path appParamPath, String reportName) throws IOException, SQLException, ClassNotFoundException {

        List<String> listOfExpectedFileNames = mf.getFileNameFromParameter(appParamPath.toUri().getRawPath());

        List<Path> listOfPathStage1 = new ArrayList<>();

        List<String> listOfTables = new ArrayList<>();

        List<String> listOfWrongNamedFiles = new ArrayList<>();

        for (Path path : listOfPath) {

            List<String> separatedFilesPath = Lists.reverse(
                    Arrays.asList(
                            path.toUri().getRawPath()
                                    .replace("[", "").replace("]", "")
                                    .split("/")));

            List<String> separatedFileName = Arrays.asList(separatedFilesPath.get(0).split("_"));

            try {

                listOfTables.add(separatedFileName.get(1));

                for (String s : listOfTables) {

                    if (listOfExpectedFileNames.contains(s)) {

                        listOfPathStage1.add(path);

                    } else {

                        listOfWrongNamedFiles.add(s);
                    }
                }
            } catch (ArrayIndexOutOfBoundsException e) {

                System.out.println("There is a problem here: " + path.toUri().getRawPath());
            }
        }

        if (listOfWrongNamedFiles.isEmpty()) {

            String line = jvp.dateFormatForInside.format(jvp.date).concat(";1;OK;");
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "1", "OK", "", "");
            mf.writeInReport(reportName, line);

        } else {

            String line = jvp.dateFormatForInside.format(jvp.date).concat(";1;KO;xxx");
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "1", "KO", "xxx", "");
            mf.writeInReport(reportName, line);

            System.out.println("MISTAKE HERE: FILE NAME NOT FOUND IN PARAMETER TABLE!");
            System.out.println(listOfWrongNamedFiles);
        }

        return listOfPathStage1.stream().distinct().collect(Collectors.toList());
    }

    /**
     * STAGE 2
     * Check when there's a .des file there's also a .dat file (and vice-versa).
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

            List<String> files = mf.getFilesPath(path.getParent()).stream().map(Path::toString).collect(Collectors.toList());

            Boolean condDat = !Collections2.filter(files, Predicates.containsPattern(".dat")).isEmpty();
            Boolean condDes = !Collections2.filter(files, Predicates.containsPattern(".des")).isEmpty();

            if (condDat && condDes) {

                System.out.println(path.getName() + " is good!");
                listOfPathStage2.add(path);

            } else if (condDat) {

                System.out.println(path.getName() + " lack its buddy .des file!");
                commentary.add(path.getName() + " lack its buddy .des file!");
                flag.add("110");

            } else if (condDes) {

                System.out.println(path.getName() + " lack its buddy .dat file!");
                commentary.add(path.getName() + " lack its buddy .dat file!");
                flag.add("111");

            } else {

                System.out.println("No description and data file found!");
                flag.add("112");
            }
        }

        if (flag.isEmpty()) {

            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "2", "OK", "", "");
            String line = jvp.dateFormatForInside.format(jvp.date).concat(";2;OK;");
            mf.writeInReport(reportName, line);

        } else {

            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "2", "KO", flag.get(0), commentary.toString());
            String line = jvp.dateFormatForInside.format(jvp.date).concat(";2;KO;" + flag.get(0));
            mf.writeInReport(reportName, line);
        }

        return listOfPathStage2;
    }

    /**
     * STAGE 3
     * Check if the files already exist in the datalake HDFS environment.
     * @param listOfPath
     * @param hdfsPath
     * @param reportName
     * @return
     * @throws IOException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    List<Path> filterFilesAlreadyExistingHdfs(List<Path> listOfPath, Path hdfsPath, String reportName) throws IOException, SQLException, ClassNotFoundException {

        List<Path> listOfPathStage3 = new ArrayList<>();

        List<String> listOfFileNamesHDFS = mf.getFilesPath(hdfsPath).stream().map(Path::getName).collect(Collectors.toList());

        List<Path> listOfFilesInDatalake = new ArrayList<>();

        System.out.println(listOfFileNamesHDFS);

        for (Path p : listOfPath) {

            if (listOfFileNamesHDFS.contains(p.getName())) {

                listOfFilesInDatalake.add(p);

            } else {

                listOfPathStage3.add(p);
            }
        }

        if (!listOfFilesInDatalake.isEmpty()) {

            String line = jvp.dateFormatForInside.format(jvp.date).concat(";3;KO;101");

            String commentary = listOfFilesInDatalake.stream().map(Path::getName).distinct().collect(Collectors.toList()) + " already exist in the datalake.";

            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "3", "KO", "101", commentary);
            mf.writeInReport(reportName, line);

        } else {

            String line = jvp.dateFormatForInside.format(jvp.date).concat(";3;OK;");
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "3", "OK", "", "");

            mf.writeInReport(reportName, line);
        }

        return listOfPathStage3;
    }

    /**
     * STAGE 4
     * Check if the .des file has allowed type.
     * @param listOfPaths
     * @param reportName
     * @return
     * @throws IOException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    List<Path> filterFilesWithoutAllowedTypes(List<Path> listOfPaths, String reportName) throws IOException, SQLException, ClassNotFoundException {

        List<String> listOfFilesToRemove = new ArrayList<>();

        List<Path> listOfPathStage4 = new ArrayList<>();

        for (Path path : listOfPaths) {

            Path desPath = mf.getDesFilePath(mf.getFilesPath(path.getParent()));

            List<String> listOfTypes = mf.getTypesFromDesFile(desPath.toUri().getRawPath());

            for (String type : listOfTypes) {

                if (!jvp.acceptedTypes.contains(type)) {

                    listOfFilesToRemove.add(path.getName());
                }
            }

            if (!listOfFilesToRemove.contains(path.getName())) {

                listOfPathStage4.add(path);
            }
        }

        if (listOfFilesToRemove.isEmpty()) {

            System.out.println("Types are fine in all tables.");
            String line = jvp.dateFormatForInside.format(jvp.date).concat(";4;OK;");
            mf.writeInReport(reportName, line);
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "4", "OK", "", "");

        } else {

            String line = jvp.dateFormatForInside.format(jvp.date).concat(";4;KO;100");
            mf.writeInReport(reportName, line);
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "4", "KO", "100", "");
        }

        return listOfPathStage4;
    }
}
