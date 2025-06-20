# KafkaStream_WeatherData
Application Kafka qui consomme des données météorologiques en temps réel, les trasnforme en Fahrenheit et les renvoie dans un topic Kafka.
Un filtre de données météorologiques est positionné pour accepter les températures supérieures à 30 degrés Celsius.

# Description

Application Kafka Streams qui traite les données météorologiques en temps réel.
collecte des données météorologiques en temps réel via Kafka. Chaque station météorologique envoie des messages dans le topic Kafka nommé 'weather-data'.
Les messages ont le format suivant :
station,temperature,humidity
- station : L'identifiant de la station (par exemple, Station1, Station2, etc.).
- temperature : La température mesurée (en °C, par exemple, 25.3).
- humidity : Le pourcentage d'humidité (par exemple, 60).


# Lancement de l'application kafka Stream
<table>
    <tr>
        <th>Programme Main.java</th>
        <th>Résultat</th>
    </tr>
    <tr>
        <td>Application Kafka Streams</td>
        <td><img src="src/main/resources/Captures/Capture_resultat_weather_data_1.png"></td>
    </tr>

</table>

# Lancement des producers et consumers kafka
<table>
    <tr>
        <th>Action</th>
        <th>Capture d'écran</th>
    </tr>
    <tr>
        <td>Docker Kafka</td>
        <td><img src="src/main/resources/Captures/Capture_weather_docker_1.png"></td>
    </tr>
   <tr>
        <td>Création des topics</td>
        <td><img src="src/main/resources/Captures/Capture_creation_topics_weather_data_1.png"></td>
    </tr>
    <tr>
        <td>Producer topic 'weather-data'</td>
        <td><img src="src/main/resources/Captures/Capture_weather_data_producer_1.png"></td>
    </tr>
    <tr>
        <td>Consumer topic 'output-weather-data' (conserve les températures > 30 °c et les convertit en Fahrenheit)</td>
        <td><img src="src/main/resources/Captures/Capture_weather_data_consumer_1.png"></td>
    </tr>
    <tr>
         <td>Consumer topic 'station-averages' (Publie les moyennes de température et d'humidité par station)</td>
        <td><img src="src/main/resources/Captures/Capture_weather_data_consumer_2.png"></td>
    </tr>

</table>