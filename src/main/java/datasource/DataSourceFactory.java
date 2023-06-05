package datasource;
import org.postgresql.ds.PGSimpleDataSource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DataSourceFactory {
    private static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/LolDB";
    private static final String USERNAME = "user_lol";
    private static final String PASSWORD = "user_lol";
    public static final String SCHEMA = "LolDB";

    public static DataSource createDataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(DATABASE_URL);
        dataSource.setUser(USERNAME);
        dataSource.setCurrentSchema(SCHEMA);
        dataSource.setPassword(PASSWORD);
        return dataSource;
    }

}

