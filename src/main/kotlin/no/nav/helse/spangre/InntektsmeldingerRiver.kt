package no.nav.helse.spangre

import no.nav.helse.rapids_rivers.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import kotlin.system.exitProcess

class InntektsmeldingerRiver(
    rapidsConnection: RapidsConnection,
    private val producer: KafkaProducer<String, String>,
) : River.PacketListener {
    private var antallIMLest = 0
    private var antallIMMedUTSLest = 0

    init {
        River(rapidsConnection).apply {
            validate { it.requireKey("inntektsmeldingId") }
            validate { it.requireKey("beregnetInntekt") }
            validate { it.requireKey("@opprettet") }
            validate { it.requireValue("@event_name", "inntektsmelding") }
            validate { it.interestedIn("refusjon.beloepPrMnd") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        if (erUtbetalingTilSøker(packet)) {
            ++antallIMMedUTSLest
            producer.send(ProducerRecord(aivenOppgaveTopicName, packet.tilOppgaveDTO()))
        }
        if (++antallIMLest % 500 == 0) log.info("Inntektsmelding nummer ${antallIMLest} lest 🐧")
        if (antallIMMedUTSLest % 500 == 0) log.info("Inntektsmelding nummer ${antallIMLest} med utbetaling til søker lest 🤒")

        if (packet["@opprettet"].asLocalDateTime() > LocalDate.of(2022, 1, 11).atStartOfDay()) {
            log.info("Antall IM lest: $antallIMLest, antall IM med utbetaling til søker lest: $antallIMMedUTSLest. Avslutter jobben.")
            exitProcess(0)
        }
    }

    private fun erUtbetalingTilSøker(packet: JsonMessage): Boolean {
        val refusjon = packet["refusjon.beloepPrMnd"].takeUnless { it.isMissingOrNull() }?.asInt()
        val inntekt = packet["beregnetInntekt"].asInt()
        return refusjon != inntekt
    }

}

private fun JsonMessage.dokumentId() =
    UUID.fromString(this["inntektsmeldingId"].asText())

private fun JsonMessage.tilOppgaveDTO(): String {
    return java.lang.String("""
        {
            "dokumentType": "Inntektsmelding",
            "oppdateringstype": "Utsett",
            "dokumentId": "${dokumentId()}",
            "timeout": "${LocalDateTime.now().plusDays(1)}"
        }
    """).replaceAll("[\\r\\n\\s]+", "")
}
