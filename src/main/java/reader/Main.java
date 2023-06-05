package reader;

import datasource.DataSourceFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) {
        Path jsonFilePath = Path.of("src/main/resources/files/championFull.json");
        DataSource dataSource = DataSourceFactory.createDataSource();
        JsonReader jsonReader = new JsonReader(jsonFilePath, dataSource);

        try {
            jsonReader.insertJsonDataIntoDB();
        } catch (IOException e) {
            System.out.println("Erreur lors de la lecture du fichier JSON : " + e.getMessage());
        } catch (SQLException e) {
            System.out.println("Erreur lors de l'insertion des données en base de données : " + e.getMessage());
        }
    }


}