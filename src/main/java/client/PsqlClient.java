package client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;

public class PsqlClient {

    private final static String HOST = "localhost";
    private final static String DATABASE = "json_benchmark";
    private final static String USER = "nik";
    private final static String PASSWORD = "llvm123";
    private final static String TABLE = "json_tables";

    private final static String APPENDIX = "SELECT COUNT(id) FROM " + TABLE;
    private final static String QUERY_1 = APPENDIX + " WHERE DATA->>'brand' = 'ACME';";
    private final static String QUERY_2 = APPENDIX
            + " WHERE (DATA::JSONB)?'name' AND DATA->>'name' = 'AC3 Case Red';";
    private final static String QUERY_3 = APPENDIX
            + " WHERE (DATA::JSONB)?& array['type', 'name', 'price'];";
    private final static String QUERY_4 = APPENDIX
            + " WHERE (DATA::JSONB) ?& array['type', 'name', 'price', 'available'] and data->>'type' = 'phone';";
    private final static String QUERY_5 = APPENDIX
            + " WHERE (DATA->'limits'->'voice'->>'n')::DECIMAL > 400;";
    private final static String QUERY_6 = APPENDIX
            + " WHERE (DATA#>>'{limits, voice, n}')::DECIMAL > 400;";
    private final static String QUERY_7 = APPENDIX
            + " WHERE (DATA::JSONB)?'color' and DATA->>'color' = 'black' and (DATA::JSONB)?'price' and (DATA->>'price')::DECIMAL = 12.5;";
    private final static String QUERY_8 = APPENDIX
            + " WHERE (DATA::JSONB)@>'{\"color\":\"black\", \"price\":12.5}';";

    private final static String[] QUERY = { QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5, QUERY_6,
            QUERY_7, QUERY_8 };

    private final static int RUNS = 10;
    final static int QUES = QUERY.length;

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://" + HOST + ":5432/" + DATABASE;
        Connection c = DriverManager.getConnection(url, USER, PASSWORD);
        for (int i = 0; i < QUES; i++) {
            ArrayList<Long> times = null;
            System.out.println(QUERY[i] + " :");
            try {
                times = runQuery(c, QUERY[i]);
            } catch (SQLException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
                continue;
            }

            System.out.println("Min time: " + Collections.min(times));
            System.out.println("Max time: " + Collections.max(times));
            System.out.println(
                    "Avg time: " + times.stream().mapToLong(n -> n).average().getAsDouble());
        }

        c.close();
    }

    private static ArrayList<Long> runQuery(Connection c, String sql) throws SQLException {
        ArrayList<Long> res = new ArrayList<Long>();

        for (int i = 0; i < RUNS; i++) {
            long time = System.currentTimeMillis();
            Statement st = c.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                int tmp = rs.getInt(1);
                System.out.println(tmp);
            }
            time = System.currentTimeMillis() - time;
            res.add(time);
        }
        return res;
    }

}
