package no.nav.helse.spangre

import no.nav.helse.rapids_rivers.*
import java.time.LocalDate
import java.util.*
import kotlin.system.exitProcess

class InntektsmeldingerRiver(rapidsConnection: RapidsConnection) :
    River.PacketListener {
    var antallIMLest = 0

    init {
        River(rapidsConnection).apply {
            validate { it.requireKey("@id") }
            validate { it.requireKey("inntektsmeldingId") }
            validate { it.requireKey("beregnetInntekt") }
            validate { it.requireKey("@opprettet") }
            validate { it.requireValue("@event_name", "inntektsmelding") }
            validate { it.interestedIn("refusjon.beloepPrMnd") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val hendelseId = UUID.fromString(packet["@id"].asText())
        val dokumentId = packet.dokumentId()

//        sjekkUtbetalingTilSøker(packet)
        if   (++antallIMLest % 500 == 0) log.info("Inntektsmelding nummer ${++antallIMLest } lest 🐧")
        if (++antallIMLest % 10000 == 0) log.info("Inntektsmelding med dato ${packet["@opprettet"].asLocalDateTime()}")

        if (packet["@opprettet"].asLocalDateTime() > LocalDate.of(2021, 10, 31).atStartOfDay()) {
            log.info("Antall IM lest: $antallIMLest. Avslutter jobben.")
            exitProcess(0)
        }
    }

    private fun sjekkUtbetalingTilSøker(packet: JsonMessage) {
        val refusjon = packet["refusjon.beloepPrMnd"].takeUnless { it.isMissingOrNull() }?.asInt()
        val inntekt = packet["beregnetInntekt"].asInt()
        if (refusjon != inntekt) log.info("Got one")
    }

    private fun JsonMessage.dokumentId() =
        UUID.fromString(this["inntektsmeldingId"].asText())
}
