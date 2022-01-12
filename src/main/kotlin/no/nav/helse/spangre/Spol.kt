package no.nav.helse.spangre

import java.io.File
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringDeserializer


class Spol(
    private val env: Map<String, String>,
    private val topic: String,
    private val groupId: String,
    private val timestamp: LocalDateTime
) {
    fun spol() {
        log.info("Spoler $topic for $groupId til $timestamp")
        KafkaConsumer(consumerConfig(env, groupId), StringDeserializer(), StringDeserializer()).use { consumer ->
            consumer.partitionsFor(topic)
                .map { TopicPartition(topic, it.partition()) }
                .also {
                    consumer.assign(it)
                    while (consumer.poll(Duration.ofSeconds(1)).count() == 0) {
                        log.debug("Poll count == 0")
                    }
                }
                .map { it to timestamp.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() }.toMap()
                .let { consumer.offsetsForTimes(it) }
                .mapValues { (_, offsetAndTimestamp) -> offsetAndTimestamp.offset() }
                .onEach { (topicPartition, offset) -> log.info("For groupId $groupId, setter offset for partisjon $topicPartition til $offset") }
                .forEach { (topicPartition, offset) -> consumer.seek(topicPartition, offset) }

            consumer.commitSync()
        }
    }
}

internal fun consumerConfig(env: Map<String, String>, groupId: String) = Properties().apply {
    putAll(kafkaBaseConfig(env))
    put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    put(ConsumerConfig.CLIENT_ID_CONFIG, env.getValue("NAIS_CLIENT_ID"))
    put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
}

private fun kafkaBaseConfig(env: Map<String, String>) = Properties().apply {
    val truststore = env.getValue("KAFKA_TRUSTSTORE_PATH")
    val truststorePassword = env.getValue("KAFKA_CREDSTORE_PASSWORD")
    val bootstrapServers = env.getValue("KAFKA_BROKERS")
    val keystoreLocation = env.getValue("KAFKA_KEYSTORE_PATH")
    val keystorePassword = env.getValue("KAFKA_CREDSTORE_PASSWORD")

    put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
    put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
    put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, File(truststore).absolutePath)
    put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword)
    put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation)
    put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword)
}
