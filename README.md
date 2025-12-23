\# 🌤️ Real-Time Weather Data Pipeline



A complete streaming data pipeline that fetches live weather data, processes it with Apache Kafka, and visualizes it in Tableau.



\## 🚀 Features

\- Real-time weather data ingestion from Open-Meteo API

\- Apache Kafka for stream processing

\- Java-based ETL transformations

\- Tableau dashboard with live updates

\- One-command deployment



\## 🏗️ Architecture



\[Weather API] → \[Kafka Producer] → \[Kafka] → \[ETL Consumer] → \[Tableau Dashboard]





\## 📦 Quick Start

1\. Clone this repository

2\. Run: `mvn clean package`

3\. Start Kafka server

4\. Run the producer and consumer

5\. Connect Tableau to: `http://localhost:4567/tableau`



\## 🛠️ Technologies

\- Java 11

\- Apache Kafka 3.6

\- Maven

\- Tableau

\- REST APIs



\## 📄 License

MIT

