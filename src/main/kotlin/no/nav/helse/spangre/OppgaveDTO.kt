package no.nav.helse.spangre

import java.time.LocalDateTime
import java.util.UUID

data class OppgaveDTO(
    val dokumentType: DokumentTypeDTO,
    val oppdateringstype: OppdateringstypeDTO,
    val dokumentId: UUID,
    val timeout: LocalDateTime
)

enum class OppdateringstypeDTO {
    Utsett
}

enum class DokumentTypeDTO {
    Inntektsmelding
}
