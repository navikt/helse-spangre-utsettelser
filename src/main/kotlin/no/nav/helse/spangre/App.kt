package no.nav.helse.spangre

import java.util.*
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.RapidsConnection.StatusListener
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

internal val log = LoggerFactory.getLogger("spangre-utsettelser")
internal const val aivenOppgaveTopicName = "tbd.spre-oppgaver"

fun main() {
    val env = System.getenv()
//    Spol(
//        env,
//        "tbd.rapid.v1",
//        "tbd-spangre-utsettelser-v1",
//        LocalDateTime.of(2021, 10, 18, 12, 0, 0)
//    ).spol()
    val rapidsConnection = launchApplication(env)
    rapidsConnection.register(object: StatusListener {
        override fun onShutdown(rapidsConnection: RapidsConnection) {
            log.info("Noe gikk galt - avslutter 📴")
            exitProcess(1)
        }
    })
    rapidsConnection.start()
}

fun launchApplication(
    environment: Map<String, String>
): RapidsConnection {
    val aivenProducer = createAivenProducer(environment)

    return RapidApplication.create(environment).apply {
        registerRivers(aivenProducer)
    }
}

internal fun RapidsConnection.registerRivers(
    producer: KafkaProducer<String, String>
) {
    InntektsmeldingerRiver(this, producer)
}

private fun createAivenProducer(env: Map<String, String>): KafkaProducer<String, String> {
    val properties = Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, env.getValue("KAFKA_BROKERS"))
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name)
        put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
        put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "jks")
        put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
        put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, env.getValue("KAFKA_TRUSTSTORE_PATH"))
        put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, env.getValue("KAFKA_CREDSTORE_PASSWORD"))
        put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, env.getValue("KAFKA_KEYSTORE_PATH"))
        put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, env.getValue("KAFKA_CREDSTORE_PASSWORD"))

        put(ProducerConfig.ACKS_CONFIG, "1")
        put(ProducerConfig.LINGER_MS_CONFIG, "0")
        put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    }
    return KafkaProducer(properties, StringSerializer(), StringSerializer())
}
