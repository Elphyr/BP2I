package BP2I.IntegrationServeurCollecte.Func;

import java.sql.*;

class JDBCFunctions {

    static private Connection JDBCConnect() throws ClassNotFoundException, SQLException {

        Class.forName("org.postgresql.Driver");

        String url = "jdbc:postgresql://localhost:5432/";
        String user = "postgres";
        String passwd = "";

        Connection con = DriverManager.getConnection(url, user, passwd);

        return con;
    }

    static void dropTable(String tableName) throws SQLException, ClassNotFoundException {

        Connection con = JDBCConnect();

        PreparedStatement dropTable = con.prepareStatement("DROP TABLE IF EXISTS " + tableName);
        dropTable.executeUpdate();
    }

    static void writeStageResultIntoTable(String tableName, String date, String stage, String result, String errorCode, String commentary) throws ClassNotFoundException, SQLException {

        Connection con = JDBCConnect();

        Statement state = con.createStatement();

        PreparedStatement createTable = con.prepareStatement("CREATE TABLE IF NOT EXISTS " + tableName +
                "(DATE varchar(225), STAGE varchar(225), RESULT varchar(225), ERROR_CODE varchar(225), COMMENTARY varchar(255), PRIMARY KEY (STAGE))");
        createTable.executeUpdate();

        PreparedStatement st = con.prepareStatement("INSERT INTO " + tableName + "(DATE, STAGE, RESULT, ERROR_CODE, COMMENTARY) VALUES (?, ?, ?, ?, ?)");
        st.setString(1, date);
        st.setString(2, stage);
        st.setString(3, result);
        st.setString(4, errorCode);
        st.setString(5, commentary);
        st.executeUpdate();
        st.close();

        ResultSet resultTable = state.executeQuery("SELECT * FROM " + tableName);
        ResultSetMetaData rsmd = resultTable.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        System.out.println(">>> " + tableName + " <<<");

        for (int i = 1; i <= rsmd.getColumnCount(); i++) System.out.print(rsmd.getColumnName(i) + "\t");
        System.out.println();

        while (resultTable.next()) {

            for (int i = 1; i <= columnsNumber; i++) {

                if (i > 1) System.out.print(",  ");
                System.out.print(resultTable.getString(i));
            }
            System.out.println();
        }

        resultTable.close();
        state.close();
    }
}