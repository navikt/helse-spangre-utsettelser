package no.nav.helse.spangre

import java.util.*
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDateTime
import no.nav.helse.rapids_rivers.isMissingOrNull
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import kotlin.system.exitProcess

class InntektsmeldingerRiver(
    rapidsConnection: RapidsConnection,
    private val producer: KafkaProducer<String, String>,
) : River.PacketListener {
    private var antallIMLest = 0
    private var antallIMMedFRLest = 0

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
        ++antallIMLest

        if (erFullRefusjon(packet)) {
            ++antallIMMedFRLest
//            producer.send(ProducerRecord(aivenOppgaveTopicName, packet.tilOppgaveDTO()))
//            log.info("Ny timeout sendt for ${packet.dokumentId()} ⏰")
            log.info("Ny timeout ville ha blitt sendt for ${packet.dokumentId()} ⏰")
        }

        if (antallIMLest % 500 == 0) log.info("Inntektsmelding nummer ${antallIMLest} lest 🐧")
        if (antallIMMedFRLest % 500 == 0) log.info("Inntektsmelding med full refusjon nummer ${antallIMMedFRLest} lest 🤒")
        if (antallIMLest % 1000 == 0) log.info("Inntektsmelding med dato ${packet.opprettet()} 📆")

//        if (packet.opprettet() > LocalDate.of(2022, 1, 14).atStartOfDay()) {
        if (antallIMMedFRLest == 1000) {
            log.info("Antall IM lest: $antallIMLest, antall IM med full refusjon lest: $antallIMMedFRLest. Avslutter jobben 💀")
            exitProcess(0)
        }
    }

    private fun erFullRefusjon(packet: JsonMessage) = !erUtbetalingTilSøker(packet)

    private fun erUtbetalingTilSøker(packet: JsonMessage): Boolean {
        val refusjon = packet["refusjon.beloepPrMnd"].takeUnless { it.isMissingOrNull() }?.asInt()
        val inntekt = packet["beregnetInntekt"].asInt()
        return refusjon != inntekt
    }
}

private fun JsonMessage.opprettet() = this["@opprettet"].asLocalDateTime()

private fun JsonMessage.dokumentId() =
    UUID.fromString(this["inntektsmeldingId"].asText())

private fun JsonMessage.tilOppgaveDTO(): String {
    return java.lang.String(
        """
        {
            "dokumentType": "Inntektsmelding",
            "oppdateringstype": "Utsett",
            "dokumentId": "${dokumentId()}",
            "timeout": "${opprettet().plusDays(40)}"
        }
    """
    ).replaceAll("[\\r\\n\\s]+", "")
}
