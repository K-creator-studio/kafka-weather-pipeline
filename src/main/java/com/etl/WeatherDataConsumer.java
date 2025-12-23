package com.etl;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.errors.WakeupException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;

public class WeatherDataConsumer {
    
    private static final String TOPIC = "live-weather-data";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "weather-etl-group-1";
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    public static void main(String[] args) {
        System.out.println("=== Starting Weather Data ETL Consumer ===");
        System.out.println("Kafka: " + BOOTSTRAP_SERVERS);
        System.out.println("Topic: " + TOPIC);
        System.out.println("Group: " + GROUP_ID);
        System.out.println("==========================================");
        
        // 1. Configure consumer properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // Start from the beginning when first joining group
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Manual commit for better control
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        // 2. Create Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        
        // 3. Subscribe to topic
        consumer.subscribe(Collections.singletonList(TOPIC));
        
        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n=== Shutting down ETL Consumer ===");
            consumer.wakeup();
        }));
        
        try {
            int processedCount = 0;
            
            while (true) {
                // 4. Poll for new messages
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    System.out.println("No new messages. Waiting...");
                    continue;
                }
                
                System.out.println("\n=== Processing " + records.count() + " new weather record(s) ===");
                
                // Start timing the batch
                long batchStartTime = System.currentTimeMillis();
                
                // 5. Process each message (ETL Pipeline)
                for (ConsumerRecord<String, String> record : records) {
                    processedCount++;
                    
                    try {
                        // ETL PROCESSING PIPELINE:
                        
                        // EXTRACT: Parse JSON from Kafka
                        JsonNode weatherData = objectMapper.readTree(record.value());
                        
                        // TRANSFORM: Process and enrich data
                        double temperature = weatherData.path("temperature").asDouble();
                        double windSpeed = weatherData.path("windspeed").asDouble();
                        int weatherCode = weatherData.path("weathercode").asInt();
                        String time = weatherData.path("time").asText();
                        
                        // Convert temperature from Celsius
                        double temperatureF = celsiusToFahrenheit(temperature);
                        
                        // Determine weather condition from code
                        String condition = getWeatherCondition(weatherCode);
                        
                        // Calculate wind chill (feels-like temperature)
                        double feelsLike = calculateWindChill(temperature, windSpeed);
                        
                        // Categorize temperature
                        String tempCategory = categorizeTemperature(temperature);
                        
                        // Generate alert if needed
                        String alert = generateAlert(temperature, windSpeed, condition);
                        
                        // Get current processing time
                        String processedTime = LocalDateTime.now().format(formatter);
                        
                        // LOAD: Create final transformed record
                        String transformedRecord = String.format(
                            "{\"original_time\": \"%s\", \"processed_time\": \"%s\", " +
                            "\"temperature_c\": %.1f, \"temperature_f\": %.1f, " +
                            "\"feels_like_c\": %.1f, \"wind_speed_kmh\": %.1f, " +
                            "\"condition\": \"%s\", \"category\": \"%s\", " +
                            "\"alert\": \"%s\", \"partition\": %d, \"offset\": %d}",
                            time, processedTime,
                            temperature, temperatureF,
                            feelsLike, windSpeed,
                            condition, tempCategory,
                            alert, record.partition(), record.offset()
                        );
                        
                        // 6. Display ETL results
                        System.out.println("\n--- Record #" + processedCount + " ---");
                        System.out.println("Original: " + record.value());
                        System.out.println("Transformed: " + transformedRecord);
                        
                        // Display human-readable summary
                        System.out.println("📊 SUMMARY:");
                        System.out.println("  🌡️  Temperature: " + temperature + "°C (" + temperatureF + "°F)");
                        System.out.println("  💨 Wind Speed: " + windSpeed + " km/h");
                        System.out.println("  😌 Feels Like: " + feelsLike + "°C");
                        System.out.println("  ⛅ Condition: " + condition);
                        System.out.println("  🏷️  Category: " + tempCategory);
                        if (!alert.isEmpty()) {
                            System.out.println("  ⚠️  ALERT: " + alert);
                        }
                        System.out.println("  📍 Source: Partition " + record.partition() + ", Offset " + record.offset());
                        
                    } catch (Exception e) {
                        System.err.println("Error processing record: " + e.getMessage());
                        System.err.println("Raw data: " + record.value());
                    }
                }
                
                // 7. Commit offsets after successful processing
                consumer.commitSync();
                
                // Calculate and display batch statistics
                long batchTime = System.currentTimeMillis() - batchStartTime;
                double recordsPerSecond = 0;
                if (batchTime > 0) {
                    recordsPerSecond = records.count() / (batchTime / 1000.0);
                }
                
                System.out.printf("\n📈 BATCH STATS: %d records in %d ms (%.1f records/sec)\n",
                    records.count(), batchTime, recordsPerSecond);
                
                System.out.println("\n✅ Successfully processed batch. Waiting for more data...");
                
                // Small pause between polls
                Thread.sleep(2000);
                
            }
            
        } catch (Exception e) {
            if (!(e instanceof WakeupException)) {
                System.err.println("Consumer error: " + e.getMessage());
                e.printStackTrace();
            }
        } finally {
            // 8. Cleanup
            consumer.close();
            System.out.println("\n=== ETL Consumer Stopped ===");
        }
    }
    
    // ========== ETL TRANSFORMATION FUNCTIONS ==========
    
    private static double celsiusToFahrenheit(double celsius) {
        return (celsius * 9/5) + 32;
    }
    
    private static String getWeatherCondition(int code) {
        // WMO Weather interpretation codes
        if (code == 0) return "Clear sky";
        if (code <= 3) return "Partly cloudy";
        if (code <= 48) return "Foggy";
        if (code <= 67) return "Rainy";
        if (code <= 77) return "Snowy";
        if (code <= 99) return "Thunderstorm";
        return "Unknown";
    }
    
    private static double calculateWindChill(double temperature, double windSpeed) {
        // Simple wind chill formula (approximate)
        if (temperature <= 10 && windSpeed > 4.8) {
            return temperature - (windSpeed * 0.7);
        }
        return temperature;
    }
    
    private static String categorizeTemperature(double temperature) {
        if (temperature <= 0) return "Freezing";
        if (temperature <= 10) return "Cold";
        if (temperature <= 20) return "Cool";
        if (temperature <= 30) return "Warm";
        return "Hot";
    }
    
    private static String generateAlert(double temperature, double windSpeed, String condition) {
        StringBuilder alerts = new StringBuilder();
        
        if (temperature <= -10) {
            alerts.append("Extreme cold warning! ");
        } else if (temperature >= 35) {
            alerts.append("Heat wave warning! ");
        }
        
        if (windSpeed > 50) {
            alerts.append("High wind warning! ");
        }
        
        if (condition.contains("Thunderstorm")) {
            alerts.append("Thunderstorm alert! ");
        }
        
        return alerts.toString().trim();
    }
}