package reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import datasource.DataSourceFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class JsonReader {
    private final Path filePath;
    private final DataSource dataSource;

    private final String schema = DataSourceFactory.SCHEMA;
    private final List<String> TABLES_TO_RESET = Arrays.asList("skins", "passive", "spells", "stats", "champions", "image");

    public JsonReader(Path filePath, DataSource dataSource) {
        this.filePath = filePath;
        this.dataSource = dataSource;
    }

    public String readJsonFile() throws IOException {
        return Files.lines(filePath)
                .collect(Collectors.joining());
    }

    public void insertJsonDataIntoDB() throws IOException, SQLException {
        String jsonContent = readJsonFile();

        // Parse le contenu JSON et récupère les informations à insérer en base de données
        // Adaptation nécessaire en fonction de la structure de votre fichier JSON
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonData = mapper.readTree(jsonContent);
        try (Connection connection = dataSource.getConnection()) {
            //clean all tables
            connection.setAutoCommit(false);
            TABLES_TO_RESET.forEach(table_name -> {
                try {
                    String delQuery = "DELETE from \"" + schema + "\"." + table_name;
                    PreparedStatement preparedStatement = connection.prepareStatement(delQuery);
                    preparedStatement.executeUpdate();
                } catch (Exception e) {
                    System.out.println("Error");
                }
            });
            // Prépare la requête SQL d'insertion
            Iterator<Map.Entry<String, JsonNode>> fieldsIterator = jsonData.get("data").fields();

            while (fieldsIterator.hasNext()) {
                Map.Entry<String, JsonNode> field = fieldsIterator.next();
                JsonNode fieldValue = field.getValue();
                insertChampions(connection, fieldValue);
                insertSkins(connection, fieldValue);
                insertStats(connection, fieldValue);
                insertSpells(connection, fieldValue);
                insertPassive(connection, fieldValue);
            }
            connection.commit(); // Valide la transaction
            System.out.println("Données insérées avec succès dans la base de données.");
        } catch (SQLException e) {
            System.out.println("Erreur lors de l'insertion des données en base de données : " + e.getMessage());
        }
    }

    private void insertChampions(Connection connection, JsonNode fieldValue) throws SQLException {
        String insertQuery = "INSERT INTO \"" + schema + "\".champions " +
                "(id, key, name, title, lore, blurb, partype,allytips,enemytips,tags,info_attack, " +
                "info_defense, info_magic, info_difficulty,image_id)" +
                " VALUES (?,?,?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

        // Insère les données dans la base de données
        // Adaptation nécessaire en fonction de la structure de votre fichier JSON
        preparedStatement.setString(1, fieldValue.get("id").asText());
        preparedStatement.setInt(2, fieldValue.get("key").asInt());
        preparedStatement.setString(3, fieldValue.get("name").asText());
        preparedStatement.setString(4, fieldValue.get("title").asText());
        preparedStatement.setString(5, fieldValue.get("lore").asText());
        preparedStatement.setString(6, fieldValue.get("blurb").asText());
        preparedStatement.setString(7, fieldValue.get("partype").asText());

        //allytips
        List<String> allytips = new ArrayList<String>();
        for (int i = 0; i < fieldValue.get("allytips").size(); i++) {
            allytips.add(fieldValue.get("allytips").get(i).asText());
        }
        String[] allytipsArray = allytips.toArray(new String[0]);
        Array allytipsSqlArray = connection.createArrayOf("varchar", allytipsArray);
        preparedStatement.setArray(8, allytipsSqlArray);
        //enemytips
        List<String> enemytips = new ArrayList<>();
        for (int i = 0; i < fieldValue.get("enemytips").size(); i++) {
            enemytips.add(fieldValue.get("enemytips").get(i).asText());
        }
        String[] enemytipsArray = enemytips.toArray(new String[0]);
        Array enemytipsSqlArray = connection.createArrayOf("varchar", enemytipsArray);
        preparedStatement.setArray(9, enemytipsSqlArray);

        //tags
        List<String> tags = new ArrayList<>();
        for (int i = 0; i < fieldValue.get("tags").size(); i++) {
            tags.add(fieldValue.get("tags").get(i).asText());
        }
        String[] tagsArray = tags.toArray(new String[0]);
        Array tagsSqlArray = connection.createArrayOf("varchar", tagsArray);
        preparedStatement.setArray(10, tagsSqlArray);
        //info
        preparedStatement.setInt(11, fieldValue.get("info").get("attack").asInt());
        preparedStatement.setInt(12, fieldValue.get("info").get("defense").asInt());
        preparedStatement.setInt(13, fieldValue.get("info").get("magic").asInt());
        preparedStatement.setInt(14, fieldValue.get("info").get("difficulty").asInt());


        preparedStatement.setInt(15, insertImage(connection, fieldValue));

        // Exécute la requête d'insertion
        preparedStatement.executeUpdate();
    }

    private int insertImage(Connection connection, JsonNode fieldValue) throws SQLException {
        String insertQuery = "INSERT INTO \"" + schema + "\".image (fullname,sprite,groupname,x,y,w,h) VALUES (?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery, Statement.RETURN_GENERATED_KEYS);
        fieldValue = fieldValue.get("image");
        preparedStatement.setString(1, fieldValue.get("full").asText());
        preparedStatement.setString(2, fieldValue.get("sprite").asText());
        preparedStatement.setString(3, fieldValue.get("group").asText());
        preparedStatement.setInt(4, fieldValue.get("x").asInt());
        preparedStatement.setInt(5, fieldValue.get("y").asInt());
        preparedStatement.setInt(6, fieldValue.get("w").asInt());
        preparedStatement.setInt(7, fieldValue.get("h").asInt());
        preparedStatement.executeUpdate();
        ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
        if (generatedKeys.next()) {
            return generatedKeys.getInt(1); // Récupérer la valeur de la première colonne (l'ID)
        }
        return 0;
    }

    private void insertSkins(Connection connection, JsonNode fieldValue) throws SQLException {
        String insertQuery = "INSERT INTO \"" + schema + "\".skins (id,championID,num,name,chromas) VALUES (?,?, ?, ?, ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

        // Insère les données dans la base de données
        for (int i = 0; i < fieldValue.get("skins").size(); i++) {
            preparedStatement.setInt(1, fieldValue.get("skins").get(i).get("id").asInt());
            preparedStatement.setString(2, fieldValue.get("id").asText());
            preparedStatement.setInt(3, fieldValue.get("skins").get(i).get("num").asInt());
            preparedStatement.setString(4, fieldValue.get("skins").get(i).get("name").asText());
            preparedStatement.setBoolean(5, fieldValue.get("skins").get(i).get("chromas").asBoolean());

            // Exécute la requête d'insertion
            preparedStatement.executeUpdate();
        }

    }

    private void insertStats(Connection connection, JsonNode fieldValue) throws SQLException {
        String insertQuery = "INSERT INTO \"" + schema + "\".stats (championid, hp, hpperlevel, mp, mpperlevel, movespeed," +
                " armor, armorperlevel, spellblock, spellblockperlevel, attackrange, hpregen, hpregenperlevel, mpregen, " +
                "mpregenperlevel, crit, critperlevel, attackdamage, attackdamageperlevel, attackspeedperlevel, attackspeed) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

        preparedStatement.setString(1, fieldValue.get("id").asText());

        fieldValue = fieldValue.get("stats");
        // Insère les données dans la base de données
        // Adaptation nécessaire en fonction de la structure de votre fichier JSON
        preparedStatement.setInt(2, fieldValue.get("hp").asInt());
        preparedStatement.setInt(3, fieldValue.get("hpperlevel").asInt());
        preparedStatement.setInt(4, fieldValue.get("mp").asInt());
        preparedStatement.setInt(5, fieldValue.get("mpperlevel").asInt());
        preparedStatement.setInt(6, fieldValue.get("movespeed").asInt());
        preparedStatement.setInt(7, fieldValue.get("armor").asInt());
        preparedStatement.setDouble(8, fieldValue.get("armorperlevel").asInt());
        preparedStatement.setInt(9, fieldValue.get("spellblock").asInt());
        preparedStatement.setDouble(10, fieldValue.get("spellblockperlevel").asInt());
        preparedStatement.setInt(11, fieldValue.get("attackrange").asInt());
        preparedStatement.setDouble(12, fieldValue.get("hpregen").asDouble());
        preparedStatement.setDouble(13, fieldValue.get("hpregenperlevel").asDouble());
        preparedStatement.setDouble(14, fieldValue.get("mpregen").asDouble());
        preparedStatement.setDouble(15, fieldValue.get("mpregenperlevel").asDouble());
        preparedStatement.setDouble(16, fieldValue.get("crit").asDouble());
        preparedStatement.setDouble(17, fieldValue.get("critperlevel").asDouble());
        preparedStatement.setDouble(18, fieldValue.get("attackdamage").asDouble());
        preparedStatement.setDouble(19, fieldValue.get("attackdamageperlevel").asDouble());
        preparedStatement.setDouble(20, fieldValue.get("attackspeedperlevel").asDouble());
        preparedStatement.setDouble(21, fieldValue.get("attackspeed").asDouble());

        // Exécute la requête d'insertion
        preparedStatement.executeUpdate();
    }

    private void insertSpells(Connection connection, JsonNode fieldValue) throws SQLException {
        String insertQuery = "INSERT INTO \"" + schema + "\".spells (id, championid, name, description, tooltip, " +
                "maxrank, cooldown, cooldownburn, cost, costburn, resource, rangeburn, leveltip_label, leveltip_effect," +
                "effect, effectburn, costtype, maxammo, range, image_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

        JsonNode spells = fieldValue.get("spells");
        for (int j = 0; j < spells.size(); j++) {
            JsonNode spell = spells.get(j);

            // Insère les données dans la base de données
            // Adaptation nécessaire en fonction de la structure de votre fichier JSON
            preparedStatement.setString(1, spell.get("id").asText());
            preparedStatement.setString(2, fieldValue.get("id").asText());
            preparedStatement.setString(3, spell.get("name").asText());
            preparedStatement.setString(4, spell.get("description").asText());
            preparedStatement.setString(5, spell.get("tooltip").asText());
            preparedStatement.setInt(6, spell.get("maxrank").asInt());

            //cooldown
            List<String> cooldown = new ArrayList<>();
            for (int i = 0; i < spell.get("cooldown").size(); i++) {
                cooldown.add(spell.get("cooldown").get(i).asText());
            }
            String[] cooldownArray = cooldown.toArray(new String[0]);
            Array cooldownSqlArray = connection.createArrayOf("double", cooldownArray);
            preparedStatement.setArray(7, cooldownSqlArray);

            preparedStatement.setString(8, spell.get("cooldownBurn").asText());

            //cost
            List<String> cost = new ArrayList<>();
            for (int i = 0; i < spell.get("cost").size(); i++) {
                cost.add(spell.get("cost").get(i).asText());
            }
            String[] costArray = cost.toArray(new String[0]);
            Array costSqlArray = connection.createArrayOf("int", costArray);
            preparedStatement.setArray(9, costSqlArray);

            preparedStatement.setInt(10, spell.get("costBurn").asInt());
            if (spell.get("resource") != null)
                preparedStatement.setString(11, spell.get("resource").asText());
            preparedStatement.setInt(12, spell.get("rangeBurn").asInt());

            //leveltipLabel
            List<String> leveltip_label = new ArrayList<>();
            if (spell.get("leveltip") != null) {
                for (int i = 0; i < spell.get("leveltip").get("label").size(); i++) {
                    leveltip_label.add(spell.get("leveltip").get("label").get(i).asText());
                }
            }

            String[] leveltip_labelArray = leveltip_label.toArray(new String[0]);
            Array leveltip_labelSqlArray = connection.createArrayOf("varchar", leveltip_labelArray);
            preparedStatement.setArray(13, leveltip_labelSqlArray);


            //leveltipEffect
            List<String> leveltip_effect = new ArrayList<>();
            if (spell.get("leveltip") != null) {
                for (int i = 0; i < spell.get("leveltip").get("effect").size(); i++) {
                    leveltip_effect.add(spell.get("leveltip").get("effect").get(i).asText());
                }
            }
            String[] leveltip_effectArray = leveltip_effect.toArray(new String[0]);
            Array leveltip_effectSqlArray = connection.createArrayOf("varchar", leveltip_effectArray);
            preparedStatement.setArray(14, leveltip_effectSqlArray);

            //effect
            List<String> effect = new ArrayList<>();
            for (int i = 0; i < spell.get("effect").size(); i++) {
                if (spell.get("effect").get(i).size() == 0)
                    effect.add(spell.get("effect").get(i).asText());
                else {
                    String tempo = "{";
                    for (int t = 0; t < spell.get("effect").get(i).size(); t++) {
                        tempo += spell.get("effect").get(i).get(t).asText();
                        if (t != spell.get("effect").get(i).size() - 1) {
                            tempo += ",";
                        }
                    }
                    tempo += "}";
                    effect.add(tempo);
                }
            }
            String[] effectArrayArray = effect.toArray(new String[0]);
            Array effectSqlArray = connection.createArrayOf("varchar", effectArrayArray);
            preparedStatement.setArray(15, effectSqlArray);

            //efffectburn
            List<String> effectburn = new ArrayList<>();
            for (int i = 0; i < spell.get("effectBurn").size(); i++) {
                effectburn.add(spell.get("effectBurn").get(i).asText());
            }
            String[] effectburnArray = effectburn.toArray(new String[0]);
            Array effectburnSqlArray = connection.createArrayOf("varchar", effectburnArray);
            preparedStatement.setArray(16, effectburnSqlArray);

            preparedStatement.setString(17, spell.get("costType").asText());
            preparedStatement.setInt(18, spell.get("maxammo").asInt());

            //range
            List<String> range = new ArrayList<>();
            for (int i = 0; i < spell.get("range").size(); i++) {
                range.add(spell.get("range").get(i).asText());
            }
            String[] rangeArray = range.toArray(new String[0]);
            Array rangeSqlArray = connection.createArrayOf("int", rangeArray);
            preparedStatement.setArray(19, rangeSqlArray);

            preparedStatement.setInt(20, insertImage(connection, fieldValue));
            // Exécute la requête d'insertion
            preparedStatement.executeUpdate();
        }

    }

    private void insertPassive(Connection connection, JsonNode fieldValue) throws SQLException {
        String insertQuery = "INSERT INTO \"" + schema + "\".passive (championID,name,description,image_id) VALUES (?, ?, ?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

        // Insère les données dans la base de données
        preparedStatement.setString(1, fieldValue.get("id").asText());
        preparedStatement.setString(2, fieldValue.get("passive").get("name").asText());
        preparedStatement.setString(3, fieldValue.get("passive").get("description").asText());
        preparedStatement.setInt(4, insertImage(connection, fieldValue));

        // Exécute la requête d'insertion
        preparedStatement.executeUpdate();


    }
}
