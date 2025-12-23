package com.etl;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class WeatherDataProducer {
    
    private static final String TOPIC = "live-weather-data";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    
    // Free weather API (no API key needed for testing)
    private static final String WEATHER_API_URL = 
        "https://api.open-meteo.com/v1/forecast?latitude=40.71&longitude=-74.01&current_weather=true";
    
    public static void main(String[] args) {
        System.out.println("Starting Weather Data Producer...");
        System.out.println("Connecting to Kafka: " + BOOTSTRAP_SERVERS);
        System.out.println("Topic: " + TOPIC);
        
        // 1. Create Kafka Producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Optional: For better reliability
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        
        // 2. Create HTTP client for API calls
        CloseableHttpClient httpClient = HttpClients.createDefault();
        ObjectMapper objectMapper = new ObjectMapper();
        
        try {
            int messageCount = 0;
            
            // Run for 10 minutes or until manually stopped
            while (messageCount < 60) { // Send 60 messages (1 per 10 seconds for 10 minutes)
                try {
                    // 3. Fetch weather data from API
                    String weatherData = fetchWeatherData(httpClient, objectMapper);
                    
                    // 4. Send to Kafka
                    ProducerRecord<String, String> record = 
                        new ProducerRecord<>(TOPIC, "weather-key", weatherData);
                    
                    producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if (e != null) {
                                System.err.println("Error sending message: " + e.getMessage());
                            } else {
                                System.out.printf("Sent message to -> Topic: %s, Partition: %d, Offset: %d%n",
                                        metadata.topic(), metadata.partition(), metadata.offset());
                            }
                        }
                    });
                    
                    // Also send synchronously for first few messages to ensure they arrive
                    if (messageCount < 3) {
                        producer.send(record).get(); // Wait for confirmation
                    }
                    
                    messageCount++;
                    System.out.println("Message #" + messageCount + " sent: " + 
                        weatherData.substring(0, Math.min(50, weatherData.length())) + "...");
                    
                    // 5. Wait before next fetch (10 seconds)
                    TimeUnit.SECONDS.sleep(10);
                    
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                    TimeUnit.SECONDS.sleep(30); // Wait longer on error
                }
            }
            
        } catch (Exception e) {
            System.err.println("Producer error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 6. Cleanup
            producer.flush();
            producer.close();
            try {
                httpClient.close();
            } catch (Exception e) {
                // Ignore
            }
            System.out.println("Producer stopped.");
        }
    }
    
    private static String fetchWeatherData(CloseableHttpClient httpClient, ObjectMapper objectMapper) 
            throws Exception {
        HttpGet request = new HttpGet(WEATHER_API_URL);
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            HttpEntity entity = response.getEntity();
            String jsonResponse = EntityUtils.toString(entity);
            
            // Parse and extract relevant data
            JsonNode rootNode = objectMapper.readTree(jsonResponse);
            JsonNode currentWeather = rootNode.path("current_weather");
            
            // Create a simplified weather record
            String weatherRecord = String.format(
                "{\"temperature\": %.1f, \"windspeed\": %.1f, \"weathercode\": %d, \"time\": \"%s\"}",
                currentWeather.path("temperature").asDouble(),
                currentWeather.path("windspeed").asDouble(),
                currentWeather.path("weathercode").asInt(),
                currentWeather.path("time").asText()
            );
            
            return weatherRecord;
        }
    }
}