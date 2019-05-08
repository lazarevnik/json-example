package client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import model.JsonData;

public class JsonClient {

    final static String APPENDIX = "SELECT COUNT(id) FROM json_tables";
    private final static String QUERY_1 = APPENDIX + " WHERE DATA->>'brand' = '\"ACME\"';";
    private final static String QUERY_2 = APPENDIX
            + " WHERE DATA?'name' AND DATA->>'name' = '\"AC3 Case Red\"';";
    private final static String QUERY_3 = APPENDIX + " WHERE DATA?&'[type, name, price]';";
    private final static String QUERY_4 = APPENDIX
            + " WHERE DATA?&'[type, name, price, available]' and data->>'type' = '\"phone\"';";
    private final static String QUERY_5 = APPENDIX + " WHERE DATA->'limits'->'voice'->>'n' > 400;";
    private final static String QUERY_6 = APPENDIX + " WHERE DATA#>>'{limits, voice, n}' > 400;";
    private final static String QUERY_7 = APPENDIX
            + " WHERE DATA?'color' and DATA->>'color' = '\"black\"' and DATA?'price' and DATA->>'price' = 12.5;";
    private final static String QUERY_8 = APPENDIX
            + " WHERE DATA@>'{\"color\":\"black\", \"price\":12.5}';";

    final static String[] QUERY = { QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5, QUERY_6, QUERY_7,
            QUERY_8 };

    private final static int RUNS = 20;
    private final static int THREADS = 20;
    final static int QUES = QUERY.length;

    static class MyCacheQuery implements Callable<Long> {
        private static SqlFieldsQuery statQuery = null;
        private static IgniteCache statCache = null;

        // public MyCacheQuery(SqlFieldsQuery query) {
        // this.query = query;
        // }

        public static void setCache(IgniteCache cache) {
            statCache = cache;
        }

        public static void setQuery(SqlFieldsQuery query) {
            statQuery = query;
        }

        @Override
        public Long call() {
            long time = System.currentTimeMillis();
            FieldsQueryCursor<?> cur = statCache.query(statQuery);
            // System.out.println(cur.getAll());
            time = System.currentTimeMillis() - time;
            return time;
        }
    }

    private static ArrayList<Long> runQuery(String sql, IgniteCache cache) {
        ArrayList<Long> res = new ArrayList<Long>();
        SqlFieldsQuery query = new SqlFieldsQuery(sql);
        MyCacheQuery.setQuery(query);
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        ArrayList<Future<Long>> list = new ArrayList<>();
        for (int i = 0; i < RUNS; i++) {
            Callable<Long> call = new MyCacheQuery();
            list.add(executor.submit(call));
        }
        executor.shutdown();
        // for (int i = 0; i < RUNS; i++) {
        // long time = System.currentTimeMillis();
        // FieldsQueryCursor<?> cur = cache.query(query);
        // System.out.println(cur.getAll());
        // time = System.currentTimeMillis() - time;
        // res.add(time);
        // }
        list.forEach(f -> {
            try {
                res.add(f.get());
            } catch (InterruptedException | ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
        return res;
    }

    public static void main(String[] args)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class.forName("org.postgresql.Driver");
        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start(args[0]);
        ignite.active(true);
        // IgniteConfiguration cfg = new IgniteConfiguration();
        try (IgniteCache<Long, JsonData> jsonCache = ignite.getOrCreateCache("jsonCache")) {
            /*
             * Loading all cache
             */
            jsonCache.clear();

            long begin = System.currentTimeMillis();
            try {
                jsonCache.loadCache(null);
                TimeUnit.SECONDS.sleep(1);
            } catch (CacheLoaderException | InterruptedException e) {
                System.err.println("Faliled to load jsonCache");
                return;
            }
            long loaded = System.currentTimeMillis();

            System.out.println("Loaded in " + (loaded - begin));
            MyCacheQuery.setCache(jsonCache);
            try {
                for (int i = 0; i < QUES; i++) {
                    ArrayList<Long> times = runQuery(QUERY[i], jsonCache);
                    System.out.println(QUERY[i] + " :");
                    System.out.println("Min time: " + Collections.min(times));
                    System.out.println("Max time: " + Collections.max(times));
                    System.out.println("Avg time: "
                            + times.stream().mapToLong(n -> n).average().getAsDouble());
                    System.out.println(
                            "Avg time: " + (times.stream().mapToLong(n -> n).sum() / RUNS));
                }
            } catch (Exception e) {
                System.err.println("Failed to execute SqlQuery");
                e.printStackTrace();
                jsonCache.close();
                ignite.close();
                return;
            }

            // SqlFieldsQuery query = null;
            // QueryCursor<List<?>> cur = null;

            // begin = System.currentTimeMillis();
            // long end = System.currentTimeMillis();
            // long full = System.currentTimeMillis();
            // try {
            // begin = System.currentTimeMillis();
            // query = new SqlFieldsQuery(
            // "select count(id) from example where data->>'brand' =
            // '\"ACME\"';");
            // cur = jsonCache.query(query);
            // System.out.println(cur.getAll());
            // end = System.currentTimeMillis();
            // System.out.println("1st in " + (end - begin) + " mills");
            //
            // begin = System.currentTimeMillis();
            // query = new SqlFieldsQuery(
            // "select count(id) from example where data?'name' and
            // data->>'name' = '\"AC3 Case Red\"';");
            // cur = jsonCache.query(query);
            // System.out.println(cur.getAll());
            // end = System.currentTimeMillis();
            // System.out.println("2nd in " + (end - begin) + " mills");
            //
            // begin = System.currentTimeMillis();
            // query = new SqlFieldsQuery(
            // "select count(id) from example where data?&'[type, name,
            // price]';");
            // cur = jsonCache.query(query);
            // System.out.println(cur.getAll());
            // end = System.currentTimeMillis();
            // System.out.println("3rd in " + (end - begin) + " mills");
            //
            // begin = System.currentTimeMillis();
            // query = new SqlFieldsQuery(
            // "select count(id) from example where data?&'[type, name, price,
            // available]' and data->>'type' = '\"phone\"';");
            // cur = jsonCache.query(query);
            // System.out.println(cur.getAll());
            // end = System.currentTimeMillis();
            // System.out.println("4th in " + (end - begin) + " mills");
            //
            // begin = System.currentTimeMillis();
            // query = new SqlFieldsQuery(
            // "select count(id) from example where
            // data->'limits'->'voice'->>'n' > 400;");
            // cur = jsonCache.query(query);
            // System.out.println(cur.getAll());
            // end = System.currentTimeMillis();
            // System.out.println("5th in " + (end - begin) + " mills");
            //
            // begin = System.currentTimeMillis();
            // query = new SqlFieldsQuery(
            // "select count(id) from example where data#>>'{limits, voice, n}'
            // > 400;");
            // cur = jsonCache.query(query);
            // System.out.println(cur.getAll());
            // end = System.currentTimeMillis();
            // System.out.println("6th in " + (end - begin) + " mills");
            //
            // begin = System.currentTimeMillis();
            // query = new SqlFieldsQuery(
            // "select count(id) from example where data?'color' and
            // data->>'color' = '\"black\"' and data?'price' and data->>'price'
            // = 12.5;");
            // cur = jsonCache.query(query);
            // System.out.println(cur.getAll());
            // end = System.currentTimeMillis();
            // System.out.println("7th in " + (end - begin) + " mills");
            //
            // begin = System.currentTimeMillis();
            // query = new SqlFieldsQuery(
            // "select count(id) from example where data@>'{\"color\":\"black\",
            // \"price\":12.5}';");
            // cur = jsonCache.query(query);
            // System.out.println(cur.getAll());
            // end = System.currentTimeMillis();
            // System.out.println("8th in " + (end - begin) + " mills");
            //
            // } catch (Exception e) {
            // System.err.println("Failed to execute SqlQuery");
            // e.printStackTrace();
            // ignite.close();
            // return;
            // }

            // long quired = System.currentTimeMillis();
            // System.out.println("[INFO] full time " + (quired - full) + "
            // mills");
        } catch (CacheException e) {
            System.err.println("Failed to get jsonCache");
            e.printStackTrace();
            ignite.close();
            return;
        }

        ignite.close();
    }

}
