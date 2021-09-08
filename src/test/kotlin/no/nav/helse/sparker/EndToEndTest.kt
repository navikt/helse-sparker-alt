package no.nav.helse.sparker;

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.mockk.every
import io.mockk.mockk
import kotliquery.queryOf
import kotliquery.sessionOf
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.flywaydb.core.Flyway
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.TempDir

import javax.sql.DataSource
import java.nio.file.Path
import java.sql.Connection
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EndToEndTest {
    private lateinit var embeddedPostgres: EmbeddedPostgres
    private lateinit var postgresConnection: Connection
    private lateinit var dataSource: DataSource
    private lateinit var flyway: Flyway
    private lateinit var meldingDao: MeldingDao
    private val producer = mockk<KafkaProducer<String, String>>(relaxed = true)

    @BeforeAll
    internal fun setupAll(@TempDir postgresPath:Path) {
        embeddedPostgres = EmbeddedPostgres.builder()
            .setOverrideWorkingDirectory(postgresPath.toFile())
            .setDataDirectory(postgresPath.resolve("datadir"))
            .start()
        postgresConnection = embeddedPostgres.postgresDatabase.connection

        dataSource = HikariDataSource(HikariConfig().apply {
            jdbcUrl = embeddedPostgres.getJdbcUrl("postgres", "postgres")
            maximumPoolSize = 3
            minimumIdle = 1
            idleTimeout = 10001
            connectionTimeout = 1000
            maxLifetime = 30001
        })

        flyway = Flyway
            .configure()
            .dataSource(dataSource)
            .load()

        meldingDao = MeldingDao(dataSource)
    }

    @BeforeEach
    fun setup() {
        flyway.clean()
        flyway.migrate()
    }

    @Test
    fun `sender meldinger på rapid med nytt @event_name`() {
        val meldingTypeId = lagreMeldingType("utbetaling_annullert")
        val fødselsnummer1 = "123456789"
        val fødselsnummer2 = "12345678"
        val fødselsnummer3 = "1234567"
        val fødselsnummer4 = "123456"
        lagreMelding("""{"@event_name": "utbetaling_annullert", "fødselsnummer": ${fødselsnummer1}}""", fødselsnummer1, meldingTypeId)
        lagreMelding("""{"@event_name": "utbetaling_annullert", "fødselsnummer": ${fødselsnummer2}}""", fødselsnummer2, meldingTypeId)
        lagreMelding("""{"@event_name": "utbetaling_annullert", "fødselsnummer": ${fødselsnummer3}}""", fødselsnummer3, meldingTypeId)
        lagreMelding("""{"@event_name": "utbetaling_annullert", "fødselsnummer": ${fødselsnummer4}}""", fødselsnummer4, meldingTypeId)

        val capture = mutableListOf<ProducerRecord<String, String>>()
        every { producer.send(capture(capture)) } returns mockk(relaxed = true)

        sendMeldinger(meldingDao, producer, meldingTypeId, "utbetaling_annullert_replay", "topic")

        assertEquals(4, capture.size)
        assertEquals(listOf(fødselsnummer1, fødselsnummer2, fødselsnummer3, fødselsnummer4), capture.map { it.key() })
        assertTrue(capture.all { objectMapper.readTree(it.value())["@event_name"].asText() == "utbetaling_annullert_replay"})
    }

    fun lagreMeldingType(type: String) = sessionOf(dataSource, returnGeneratedKey = true).use { session ->
        @Language("PostgreSQL")
        val query = "INSERT INTO melding_type(navn) VALUES(?);"
        session.run(queryOf(query, type).asUpdateAndReturnGeneratedKey)!!
    }

    fun lagreMelding(@Language("JSON") melding: String, fødselsnummer: String, meldingTypeId: Long) {
        @Language("PostgreSQL")
        val query = "INSERT INTO melding(id, melding_type_id, fnr, json) VALUES(?, ?, ?, ?::json);"
        sessionOf(dataSource).use { session ->
            session.run(queryOf(query, UUID.randomUUID(), meldingTypeId, fødselsnummer.toLong(), melding).asUpdate)
        }
    }

}
