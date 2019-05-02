package client;

import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import model.JsonData;

public class JsonClient {

    public static void main(String[] args)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        // Class.forName("org.postgresql.Driver");
        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start(args[0]);
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
            SqlFieldsQuery query = null;
            QueryCursor<List<?>> cur = null;

            begin = System.currentTimeMillis();
            long end = System.currentTimeMillis();
            long full = System.currentTimeMillis();
            try {
                begin = System.currentTimeMillis();
                query = new SqlFieldsQuery(
                        "select count(id) from example where data->>'brand' = '\"ACME\"';");
                cur = jsonCache.query(query);
                System.out.println(cur.getAll());
                end = System.currentTimeMillis();
                System.out.println("1st in " + (end - begin) + " mills");

                begin = System.currentTimeMillis();
                query = new SqlFieldsQuery(
                        "select count(id) from example where data?'name' and data->>'name' = '\"AC3 Case Red\"';");
                cur = jsonCache.query(query);
                System.out.println(cur.getAll());
                end = System.currentTimeMillis();
                System.out.println("2nd in " + (end - begin) + " mills");

                begin = System.currentTimeMillis();
                query = new SqlFieldsQuery(
                        "select count(id) from example where data?&'[type, name, price]';");
                cur = jsonCache.query(query);
                System.out.println(cur.getAll());
                end = System.currentTimeMillis();
                System.out.println("3rd in " + (end - begin) + " mills");

                begin = System.currentTimeMillis();
                query = new SqlFieldsQuery(
                        "select count(id) from example where data?&'[type, name, price, available]' and data->>'type' = '\"phone\"';");
                cur = jsonCache.query(query);
                System.out.println(cur.getAll());
                end = System.currentTimeMillis();
                System.out.println("4th in " + (end - begin) + " mills");

                begin = System.currentTimeMillis();
                query = new SqlFieldsQuery(
                        "select count(id) from example where data->'limits'->'voice'->>'n' > 400;");
                cur = jsonCache.query(query);
                System.out.println(cur.getAll());
                end = System.currentTimeMillis();
                System.out.println("5th in " + (end - begin) + " mills");

                begin = System.currentTimeMillis();
                query = new SqlFieldsQuery(
                        "select count(id) from example where data#>>'{limits, voice, n}' > 400;");
                cur = jsonCache.query(query);
                System.out.println(cur.getAll());
                end = System.currentTimeMillis();
                System.out.println("6th in " + (end - begin) + " mills");

                begin = System.currentTimeMillis();
                query = new SqlFieldsQuery(
                        "select count(id) from example where data?'color' and data->>'color' = '\"black\"' and data?'price' and data->>'price' = 12.5;");
                cur = jsonCache.query(query);
                System.out.println(cur.getAll());
                end = System.currentTimeMillis();
                System.out.println("7th in " + (end - begin) + " mills");

                begin = System.currentTimeMillis();
                query = new SqlFieldsQuery(
                        "select count(id) from example where data@>'{\"color\":\"black\", \"price\":12.5}';");
                cur = jsonCache.query(query);
                System.out.println(cur.getAll());
                end = System.currentTimeMillis();
                System.out.println("8th in " + (end - begin) + " mills");

            } catch (Exception e) {
                System.err.println("Failed to execute SqlQuery");
                e.printStackTrace();
                ignite.close();
                return;
            }

            long quired = System.currentTimeMillis();
            System.out.println("[INFO] full time " + (quired - full) + " mills");
        } catch (CacheException e) {
            System.err.println("Failed to get jsonCache");
            e.printStackTrace();
            ignite.close();
            return;
        }

        ignite.close();
    }

}
