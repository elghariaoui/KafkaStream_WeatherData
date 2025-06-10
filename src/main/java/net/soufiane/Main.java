package net.soufiane;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

/*
@author Soufiane Elghariaoui
Application Kafka Streams qui traite les données météorologiques en temps réel.
collecte des données météorologiques en temps réel via Kafka. Chaque station météorologique envoie des messages dans le topic Kafka nommé 'weather-data'.
Les messages ont le format suivant :
station,temperature,humidity
- station : L'identifiant de la station (par exemple, Station1, Station2, etc.).
- temperature : La température mesurée (en °C, par exemple, 25.3).
- humidity : Le pourcentage d'humidité (par exemple, 60).
 */
public class Main {
    public static void main(String[] args) {

        // Configurer l'application Kafka Streams
        Properties props = new Properties();
        props.put("application.id", "kafka-streams-weather-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", Serdes.String().getClass());
        props.put("default.value.serde", Serdes.String().getClass());

        // Construire le flux

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream("weather-data");

        // Afficher les données entrantes pour le débogage
        sourceStream.foreach((key, value) -> {
            System.out.println("Received message: key=" + key + ", value=" + value);
        });

        //Filtrer les données de température élevée et conserver que les températures supérieures à 30°C
        KStream<String, String> highTempStream = sourceStream
                .filter((key, value) -> {
                    String[] parts = value.split(",");
                    if (parts.length == 3) {
                        try {
                            double temperature = Double.parseDouble(parts[1]);
                            System.out.println("Temperature: " + temperature);
                            return temperature >= 30.0; // Filtrer les températures supérieures à 30°C
                        } catch (NumberFormatException e) {
                            return false; // Ignorer les lignes mal formatées
                        }
                    }
                    return false; // Ignorer les lignes mal formatées
                });

        // Envoyer les données filtrées vers le topic Kafka "output-weather-data"
        //highTempStream.to("weather-data");

        // Convertir les températures en Fahrenheit
        KStream<String, String> fahrenheitStream = highTempStream.mapValues(value -> {
            String[] parts = value.split(",");
            if (parts.length == 3) {
                try {
                    double temperatureCelsius = Double.parseDouble(parts[1]);
                    double temperatureFahrenheit = (temperatureCelsius * 9/5) + 32;
                    return parts[0] + "," + temperatureFahrenheit + "," + parts[2]; // Conserver l'humidité
                } catch (NumberFormatException e) {
                    return value; // Retourner la valeur originale en cas d'erreur de format
                }
            }
            return value; // Retourner la valeur originale si mal formatée
        });

        // Afficher les données converties pour le débogage
        fahrenheitStream.foreach((key, value) -> {
            System.out.println("Converted message: key=" + key + ", value=" + value);
        });
        // Envoyer les données filtrées vers le topic Kafka "output-weather-data"
        fahrenheitStream.to("output-weather-data");

        // Grouper les données par station et Calcule la température moyenne et le taux d'humidité moyen pour chaque station.
        // Agrégation correcte : sommeTemp, sommeHum, compteur
        KTable<String, String> averageTable = fahrenheitStream
                .groupBy((key, value) -> {
                    String[] parts = value.split(",");
                    return parts[0]; // station
                })
                .aggregate(
                        () -> "0,0,0", // sommeTemp, sommeHum, compteur
                        (key, value, aggregate) -> {
                            try {
                                String[] parts = value.split(",");
                                String[] aggParts = aggregate.split(",");
                                if (parts.length != 3 || aggParts.length != 3) return aggregate;

                                double sommeTemp = Double.parseDouble(aggParts[0]);
                                double sommeHum = Double.parseDouble(aggParts[1]);
                                int compteur = Integer.parseInt(aggParts[2]);

                                double newTemp = Double.parseDouble(parts[1]);
                                double newHum = Double.parseDouble(parts[2]);

                                // Vérification des bornes pour éviter les débordements
                                if (Double.isNaN(newTemp) || Double.isInfinite(newTemp) ||
                                        Double.isNaN(newHum) || Double.isInfinite(newHum)) {
                                    System.err.println("Valeur non valide ignorée : " + value);
                                    return aggregate;
                                }

                                sommeTemp += newTemp;
                                sommeHum += newHum;
                                compteur++;

                                return sommeTemp + "," + sommeHum + "," + compteur;
                            } catch (Exception e) {
                                System.err.println("Erreur d'agrégation : " + e.getMessage());
                                return "0,0,0"; // Retourne une valeur par défaut en cas d'erreur
                            }
                        }
                );

        // Afficher les données agrégées pour le débogage
        averageTable.toStream().foreach((key, value) -> {
            System.out.println("averageTable Aggregated message: key=" + key + ", value=" + value);
        });
        // Calculer la moyenne et formater la sortie
        KStream<String, String> averageStream = averageTable
                .toStream()
                .mapValues((station, value) -> {
                    System.out.println("Station: " + station);
                    System.out.println("Value: " + value);
                    String[] parts = value.split(",");
                    double sommeTemp = Double.parseDouble(parts[0]);
                    double sommeHum = Double.parseDouble(parts[1]);
                    int compteur = Integer.parseInt(parts[2]);
                    double avgTemp = compteur > 0 ? sommeTemp / compteur : 0;
                    double avgHum = compteur > 0 ? sommeHum / compteur : 0;
                    //return station + "," + avgTemp + "," + avgHum;
                    return station + " : Température Moyenne = " + String.format("%.2f", avgTemp) + "°F, Humidité Moyenne = " +  String.format("%.2f", avgHum)+ "%";
                });
        // Afficher les données agrégées pour le débogage
        averageStream.foreach((key, value) -> {
            System.out.println("averageStream Aggregated message: key=" + key + ", value=" + value);
        });

        //Publier les moyennes de température et d'humidité par station dans le topic 'station-averages'.
        // Envoyer les données agrégées vers le topic Kafka "output-weather-data"
        averageStream.to("station-averages");

        // Démarrer l'application Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Ajouter un hook pour arrêter proprement
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}