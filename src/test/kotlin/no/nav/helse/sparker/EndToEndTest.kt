package no.nav.helse.sparker;

import io.mockk.every
import io.mockk.mockk
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals

import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EndToEndTest {
    private var meldingDao = mockk<MeldingDao>()
    private val producer = mockk<KafkaProducer<String, String>>(relaxed = true)

    @Test
    fun `sender meldinger på rapid med riktige felter`() {
        val fodselsnumre = listOf("20046912345", "05046912345")
        val capture = mutableListOf<ProducerRecord<String, String>>()
        every { producer.send(capture(capture)) } returns mockk(relaxed = true)
        every { meldingDao.hentMeldinger() } returns listOf("20046912345", "05046912345")

        sendMeldinger(meldingDao, producer, "tbd.rapid.v1")

        assertEquals(2, capture.size)
        assertEquals(fodselsnumre, capture.map { it.key() })

        capture.forEachIndexed { index, record ->
            val event = objectMapper.readTree(record.value())
            assertDoesNotThrow {
                UUID.fromString(event["@id"].asText())
            }
            assertEquals("adressebeskyttelse_endret", event["@event_name"].asText())
            assertEquals(fodselsnumre[index], event["fødselsnummer"].asText())
        }
    }
    
}
