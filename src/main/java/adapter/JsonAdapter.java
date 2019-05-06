package adapter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.SpringResource;
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
                    return new JsonData(rs.getLong(1), rs.getString(2));
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
                stmt.setString(1, json.getData());
                stmt.setLong(2, json.getId());
                updated = stmt.executeUpdate();
            } catch (SQLException e) {
                throw new CacheLoaderException("Failed to update object [key=" + key + ']', e);
            }
            if (updated == 0) {
                try (PreparedStatement stmt = conn
                        .prepareStatement("insert into " + TABLE + " (id, data) values (?, ?)")) {
                    stmt.setLong(1, json.getId());
                    stmt.setString(2, json.getData());

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
                while (rs.next()) {
                    Long key = rs.getLong(1);
                    String data = rs.getString(2);
                    clo.apply(key, new JsonData(key, data));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
