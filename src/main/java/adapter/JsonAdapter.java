package adapter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.SpringResource;
import org.h2.util.json.JSONStringSource;
import org.h2.util.json.JSONValue;
import org.h2.util.json.JSONValueTarget;
import model.JsonData;

public class JsonAdapter extends CacheStoreAdapter<Long, JsonData> {

    @SpringResource(resourceName = "dataSource")
    private DataSource dataSource;

    /**
     * Size of loaded blocks to control heap usage
     */
    private int batchSize = 100000;

    private final String TABLE = "json_tables";

    @Override
    public JsonData load(Long key) throws CacheLoaderException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn
                    .prepareStatement("select * from " + TABLE + " where id = ?")) {
                st.setLong(1, key);
                ResultSet rs = st.executeQuery();
                if (rs.next()) {
                    String value = rs.getString(2);
                    JSONValueTarget target = new JSONValueTarget();
                    JSONStringSource.parse(value, target);
                    JSONValue parsed = target.getResult();
                    return new JsonData(rs.getLong(1), parsed);
                } else {
                    return null;
                }
            } catch (SQLException e) {
                throw new CacheLoaderException("Failed to load object [key=" + key + ']', e);
            }
        } catch (SQLException e) {
            throw new CacheLoaderException(
                    "Failed to get connection when load object [key=" + key + ']', e);
        }
    }

    @Override
    public void write(Entry<? extends Long, ? extends JsonData> entry) throws CacheWriterException {
        Long key = entry.getKey();
        JsonData json = entry.getValue();
        try (Connection conn = dataSource.getConnection()) {
            int updated;
            try (PreparedStatement stmt = conn
                    .prepareStatement("update " + TABLE + " set data = ? where id = ?")) {
                stmt.setString(1, json.getData().toString());
                stmt.setLong(2, json.getId());
                updated = stmt.executeUpdate();
            } catch (SQLException e) {
                throw new CacheLoaderException("Failed to update object [key=" + key + ']', e);
            }
            if (updated == 0) {
                try (PreparedStatement stmt = conn
                        .prepareStatement("insert into " + TABLE + " (id, data) values (?, ?)")) {
                    stmt.setLong(1, json.getId());
                    stmt.setString(2, json.getData().toString());

                    stmt.executeUpdate();
                } catch (SQLException e) {
                    throw new CacheLoaderException("Failed to insert object [key=" + key + ']', e);
                }
            }
        } catch (SQLException e) {
            throw new CacheLoaderException(
                    "Failed to get connection when write object [key=" + key + ']', e);
        }
    }

    @Override
    public void delete(Object key) throws CacheWriterException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement stmt = conn
                    .prepareStatement("delete from " + TABLE + " where id=?")) {
                stmt.setInt(1, (Integer) key);
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new CacheWriterException("Failed to delete object [key=" + key + ']', e);
            }
        } catch (SQLException e) {
            throw new CacheWriterException(
                    "Failed to get connection when delete object [key=" + key + ']', e);
        }
    }

    @Override
    public void loadCache(IgniteBiInClosure<Long, JsonData> clo, Object... args)
            throws CacheLoaderException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            stmt.setFetchSize(batchSize);
            try (ResultSet rs = stmt.executeQuery("select * from " + TABLE + ";")) {
                ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
                while (rs.next()) {
                    String value = rs.getString(2);
                    JSONValueTarget target = new JSONValueTarget();
                    JSONStringSource.parse(value, target);
                    JSONValue parsed = target.getResult();
                    CloApply task = new CloApply(clo, rs.getLong(1), parsed);
                    executor.execute(task);
                    // clo.apply(key, new JsonData(key, data));
                }
                executor.shutdown();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    class CloApply implements Runnable {

        private IgniteBiInClosure<Long, JsonData> clo;
        private Long key;
        private JSONValue data;

        CloApply(IgniteBiInClosure<Long, JsonData> clo, Long key, JSONValue data) {
            this.clo = clo;
            this.key = key;
            this.data = data;
        }

        @Override
        public void run() {
            clo.apply(key, new JsonData(key, data));
        }

    }

}
