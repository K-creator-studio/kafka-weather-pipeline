@echo off
echo ========================================
echo   KAFKA WEATHER PIPELINE - LOCAL DEPLOY
echo ========================================
echo.

REM Check if Kafka is installed
echo [1/6] Checking Kafka installation...
if not exist "C:\kafka\bin\windows\kafka-server-start.bat" (
    echo ERROR: Kafka not found at C:\kafka
    echo Please install Kafka to C:\kafka
    pause
    exit /b 1
)
echo ✅ Kafka found

REM Build the project
echo [2/6] Building Java project...
call mvn clean package -q
if errorlevel 1 (
    echo ERROR: Maven build failed
    pause
    exit /b 1
)
echo ✅ Build successful

REM Start Kafka
echo [3/6] Starting Kafka server...
start "Kafka Server" cmd /k "cd /d C:\kafka && echo [KAFKA] Starting... && bin\windows\kafka-server-start.bat config\kraft\server.properties"
echo Waiting 20 seconds for Kafka to start...
timeout /t 20 /nobreak >nul

REM Create topic if not exists
echo [4/6] Creating Kafka topic...
cd /d C:\kafka
bin\windows\kafka-topics.bat --create --topic live-weather-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists >nul 2>&1
echo ✅ Topic ready

REM Start services
echo [5/6] Starting all services...
cd /d "%~dp0"

echo Starting Weather Producer...
start "Weather Producer" cmd /k "java -cp "target/classes;target/dependency/*" com.etl.WeatherDataProducer"

timeout /t 5 /nobreak >nul

echo Starting ETL Consumer...
start "ETL Consumer" cmd /k "java -cp "target/classes;target/dependency/*" com.etl.WeatherDataConsumer"

timeout /t 5 /nobreak >nul

echo Starting Tableau API Server...
start "Tableau API" cmd /k "java -cp "target/classes;target/dependency/*" com.etl.TableauApiServer"

echo [6/6] Waiting for services to initialize...
timeout /t 10 /nobreak >nul

echo.
echo ========================================
echo          🚀 DEPLOYMENT COMPLETE!
echo ========================================
echo.
echo ✅ All services are running:
echo.
echo 1. 🌐 KAFKA SERVER    - localhost:9092
echo 2. 📡 WEATHER PRODUCER - Fetching data every 10s
echo 3. 🔄 ETL CONSUMER    - Processing transformations
echo 4. 📊 TABLEAU API     - http://localhost:4567
echo.
echo 📋 TABLEAU CONNECTION:
echo   1. Open Tableau Public
echo   2. Choose "Web Data Connector"
echo   3. Enter: http://localhost:4567/tableau
echo   4. Click "Get Weather Data"
echo.
echo Press any key to STOP all services...
pause >nul

REM Stop all services
echo Stopping all services...
taskkill /FI "WINDOWTITLE eq Kafka Server" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Weather Producer" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq ETL Consumer" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Tableau API" /F >nul 2>&1

echo All services stopped.
pause