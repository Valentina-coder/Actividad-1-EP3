package com.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

/**
 * Controlador REST que envía mensajes a Kafka.
 *
 * Demuestra dos conceptos:
 *
 * - Sin Key: distribución automática entre particiones.
 * - Con Key: los mensajes con la misma clave se envían
 *   a la misma partición.
 */

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public KafkaController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Endpoint para enviar mensajes a Kafka.
     *
     * Ejemplos de uso:
     *
     * SIN KEY:
     * http://localhost:8888/api/kafka/send?message=Mensaje1
     *
     * CON KEY:
     * http://localhost:8888/api/kafka/send?message=Mensaje1&key=orden-555
     */
    @GetMapping("/send")
    public String sendMessage(
            @RequestParam String message,
            @RequestParam(required = false) String key) {

        String topicName = "tutorial-particiones";

        if (key != null && !key.isEmpty()) {

            // Con Key: Kafka utiliza el hash de la clave
            // para determinar la partición.
            // Los mensajes con la misma key van a la misma partición.
            kafkaTemplate.send(topicName, key, message);

            return "Mensaje enviado al topic '" + topicName
                    + "' con KEY: " + key
                    + " | MENSAJE: " + message;

        } else {

            // Sin Key: Kafka utiliza su estrategia de particionamiento
            // para distribuir los mensajes automáticamente.
            kafkaTemplate.send(topicName, message);

            return "Mensaje enviado al topic '" + topicName
                    + "' (sin key - distribución automática)"
                    + " | MENSAJE: " + message;
        }
    }

    /**
     * Endpoint de verificación.
     */
    @GetMapping("/health")
    public String health() {
        return "Aplicación de Kafka Tutorial está corriendo en puerto 8888";
    }

}
```