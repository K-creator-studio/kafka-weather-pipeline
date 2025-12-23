package com.etl;

import static spark.Spark.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TableauApiServer {
    
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static List<Map<String, Object>> weatherData = Collections.synchronizedList(new ArrayList<>());
    
    public static void main(String[] args) {
        System.out.println("=== Starting Tableau Web Data Connector Server ===");
        
        // Pre-load sample data
        initializeSampleData();
        
        // Start data simulator
        startDataSimulator();
        
        // Configure Spark
        port(4567);
        
        // Enable CORS for Tableau
        enableCORS("*", "*", "*");
        
        // 1. WDC HTML PAGE (This is what Tableau connects to)
        get("/tableau", (req, res) -> {
            res.type("text/html");
            return getWdcHtml();
        });
        
        // 2. JSON DATA ENDPOINT (WDC calls this)
        get("/weather", (req, res) -> {
            res.type("application/json");
            
            // Create a copy to avoid concurrent modification
            List<Map<String, Object>> dataCopy;
            synchronized(weatherData) {
                dataCopy = new ArrayList<>(weatherData);
            }
            
            return mapper.writeValueAsString(dataCopy);
        });
        
        // 3. Test endpoint
        get("/test", (req, res) -> {
            res.type("application/json");
            return "[{\"id\":1,\"temp\":25.5},{\"id\":2,\"temp\":26.0}]";
        });
        
        // 4. Health check
        get("/health", (req, res) -> {
            res.type("application/json");
            return "{\"status\":\"running\",\"records\":\"" + weatherData.size() + "\"}";
        });
        
        System.out.println("\n✅ SERVER READY!");
        System.out.println("==========================================");
        System.out.println("📊 TABLEAU CONNECTION INSTRUCTIONS:");
        System.out.println("1. Open Tableau Public");
        System.out.println("2. Choose 'Web Data Connector'");
        System.out.println("3. Enter this EXACT URL:");
        System.out.println("   → http://localhost:4567/tableau");
        System.out.println("4. Click the 'Get Weather Data' button");
        System.out.println("5. Click 'Update Now' in Tableau");
        System.out.println("==========================================");
        System.out.println("\n📡 Endpoints:");
        System.out.println("• WDC Page:    http://localhost:4567/tableau");
        System.out.println("• Weather API: http://localhost:4567/weather");
        System.out.println("• Test:        http://localhost:4567/test");
        System.out.println("• Health:      http://localhost:4567/health");
    }
    
    private static String getWdcHtml() {
        return "<!DOCTYPE html>\n" +
               "<html>\n" +
               "<head>\n" +
               "    <title>Kafka Weather Data Connector</title>\n" +
               "    <meta charset=\"UTF-8\">\n" +
               "    <!-- MUST include Tableau WDC library -->\n" +
               "    <script src=\"https://connectors.tableau.com/libs/tableauwdc-2.3.latest.js\"></script>\n" +
               "    <script src=\"https://code.jquery.com/jquery-3.6.0.min.js\"></script>\n" +
               "    <style>\n" +
               "        body { font-family: Arial, sans-serif; margin: 40px; }\n" +
               "        h1 { color: #2c3e50; }\n" +
               "        button {\n" +
               "            background-color: #3498db;\n" +
               "            color: white;\n" +
               "            padding: 15px 30px;\n" +
               "            border: none;\n" +
               "            border-radius: 5px;\n" +
               "            font-size: 16px;\n" +
               "            cursor: pointer;\n" +
               "        }\n" +
               "        button:hover { background-color: #2980b9; }\n" +
               "        .info { background: #f8f9fa; padding: 20px; border-radius: 5px; margin: 20px 0; }\n" +
               "    </style>\n" +
               "    <script>\n" +
               "        (function() {\n" +
               "            // Create Tableau connector\n" +
               "            var myConnector = tableau.makeConnector();\n" +
               "            \n" +
               "            // Define the schema (columns)\n" +
               "            myConnector.getSchema = function(schemaCallback) {\n" +
               "                var cols = [\n" +
               "                    { id: \"id\", alias: \"ID\", dataType: tableau.dataTypeEnum.int },\n" +
               "                    { id: \"timestamp\", alias: \"Timestamp\", dataType: tableau.dataTypeEnum.string },\n" +
               "                    { id: \"temperature_c\", alias: \"Temperature (°C)\", dataType: tableau.dataTypeEnum.float },\n" +
               "                    { id: \"temperature_f\", alias: \"Temperature (°F)\", dataType: tableau.dataTypeEnum.float },\n" +
               "                    { id: \"wind_speed\", alias: \"Wind Speed (km/h)\", dataType: tableau.dataTypeEnum.float },\n" +
               "                    { id: \"feels_like\", alias: \"Feels Like (°C)\", dataType: tableau.dataTypeEnum.float },\n" +
               "                    { id: \"condition\", alias: \"Weather Condition\", dataType: tableau.dataTypeEnum.string },\n" +
               "                    { id: \"category\", alias: \"Temperature Category\", dataType: tableau.dataTypeEnum.string }\n" +
               "                ];\n" +
               "                \n" +
               "                var tableSchema = {\n" +
               "                    id: \"weatherData\",\n" +
               "                    alias: \"Live Weather Data from Kafka\",\n" +
               "                    columns: cols\n" +
               "                };\n" +
               "                \n" +
               "                schemaCallback([tableSchema]);\n" +
               "            };\n" +
               "            \n" +
               "            // Fetch data from our API\n" +
               "            myConnector.getData = function(table, doneCallback) {\n" +
               "                $.getJSON(\"http://localhost:4567/weather\", function(resp) {\n" +
               "                    var tableData = [];\n" +
               "                    \n" +
               "                    // Process each record\n" +
               "                    for (var i = 0; i < resp.length; i++) {\n" +
               "                        tableData.push({\n" +
               "                            \"id\": resp[i].id || i,\n" +
               "                            \"timestamp\": resp[i].timestamp || \"\",\n" +
               "                            \"temperature_c\": resp[i].temperature_c || 0,\n" +
               "                            \"temperature_f\": resp[i].temperature_f || 0,\n" +
               "                            \"wind_speed\": resp[i].wind_speed || 0,\n" +
               "                            \"feels_like\": resp[i].feels_like || 0,\n" +
               "                            \"condition\": resp[i].condition || \"Unknown\",\n" +
               "                            \"category\": resp[i].category || \"Unknown\"\n" +
               "                        });\n" +
               "                    }\n" +
               "                    \n" +
               "                    // Add rows to Tableau\n" +
               "                    table.appendRows(tableData);\n" +
               "                    doneCallback();\n" +
               "                }).fail(function() {\n" +
               "                    alert(\"Failed to fetch data from API. Make sure the server is running!\");\n" +
               "                });\n" +
               "            };\n" +
               "            \n" +
               "            // Register the connector with Tableau\n" +
               "            tableau.registerConnector(myConnector);\n" +
               "            \n" +
               "            // Setup button click event\n" +
               "            $(document).ready(function() {\n" +
               "                $(\"#submitButton\").click(function() {\n" +
               "                    tableau.connectionName = \"Live Kafka Weather Data\";\n" +
               "                    tableau.submit();\n" +
               "                });\n" +
               "            });\n" +
               "        })();\n" +
               "    </script>\n" +
               "</head>\n" +
               "<body>\n" +
               "    <h1>🌤️ Kafka Weather Data Connector</h1>\n" +
               "    \n" +
               "    <div class=\"info\">\n" +
               "        <p><strong>This connector fetches live weather data from:</strong></p>\n" +
               "        <ol>\n" +
               "            <li>Weather API → Kafka Producer</li>\n" +
               "            <li>Kafka Topic → ETL Consumer</li>\n" +
               "            <li>REST API → Tableau Dashboard</li>\n" +
               "        </ol>\n" +
               "        <p><strong>Current records in memory:</strong> " + weatherData.size() + "</p>\n" +
               "    </div>\n" +
               "    \n" +
               "    <p>Click the button below to import weather data into Tableau:</p>\n" +
               "    <button id=\"submitButton\">📊 Get Weather Data</button>\n" +
               "    \n" +
               "    <div style=\"margin-top: 30px; color: #666; font-size: 14px;\">\n" +
               "        <p><strong>Note:</strong> Make sure the Kafka producer and this API server are running.</p>\n" +
               "        <p>Data updates every 10 seconds automatically.</p>\n" +
               "    </div>\n" +
               "</body>\n" +
               "</html>";
    }
    
    private static void initializeSampleData() {
        for (int i = 0; i < 20; i++) {
            addNewWeatherRecord();
        }
        System.out.println("📊 Pre-loaded " + weatherData.size() + " sample records");
    }
    
    private static void startDataSimulator() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                addNewWeatherRecord();
            }
        }, 0, 10000); // Add new record every 10 seconds
    }
    
    private static void addNewWeatherRecord() {
        Random rand = new Random();
        Map<String, Object> record = new HashMap<>();
        
        double temp = -5 + rand.nextDouble() * 15; // -5 to +10°C
        double wind = 5 + rand.nextDouble() * 20; // 5-25 km/h
        double feelsLike = temp - (wind * 0.7);
        
        record.put("id", weatherData.size() + 1);
        record.put("timestamp", LocalDateTime.now().format(formatter));
        record.put("temperature_c", Math.round(temp * 10.0) / 10.0);
        record.put("temperature_f", Math.round((temp * 9/5 + 32) * 10.0) / 10.0);
        record.put("wind_speed", Math.round(wind * 10.0) / 10.0);
        record.put("feels_like", Math.round(feelsLike * 10.0) / 10.0);
        record.put("condition", getRandomCondition());
        record.put("category", categorizeTemperature(temp));
        
        synchronized(weatherData) {
            weatherData.add(record);
            // Keep only last 1000 records
            if (weatherData.size() > 1000) {
                weatherData.remove(0);
            }
        }
        
        System.out.println("➕ Added record #" + record.get("id") + " | Temp: " + temp + "°C | Condition: " + record.get("condition"));
    }
    
    private static String getRandomCondition() {
        String[] conditions = {"Clear sky", "Partly cloudy", "Cloudy", "Light rain", "Snow", "Foggy"};
        return conditions[new Random().nextInt(conditions.length)];
    }
    
    private static String categorizeTemperature(double temp) {
        if (temp <= 0) return "Freezing";
        if (temp <= 10) return "Cold";
        if (temp <= 20) return "Cool";
        return "Warm";
    }
    
    // Enable CORS for Tableau
    private static void enableCORS(final String origin, final String methods, final String headers) {
        before((request, response) -> {
            response.header("Access-Control-Allow-Origin", origin);
            response.header("Access-Control-Request-Method", methods);
            response.header("Access-Control-Allow-Headers", headers);
        });
    }
}