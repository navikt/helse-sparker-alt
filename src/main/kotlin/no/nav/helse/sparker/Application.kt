package no.nav.helse.sparker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.system.exitProcess

val objectMapper: ObjectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)


fun main() {
    val env = System.getenv()

    val config = KafkaConfig(
        bootstrapServers = env.getValue("KAFKA_BROKERS"),
        truststore = env.getValue("KAFKA_TRUSTSTORE_PATH"),
        truststorePassword = env.getValue("KAFKA_CREDSTORE_PASSWORD"),
        keystoreLocation = env.getValue("KAFKA_KEYSTORE_PATH"),
        keystorePassword = env.getValue("KAFKA_CREDSTORE_PASSWORD")
    )
    val topic = env.getValue("KAFKA_TARGET_TOPIC")

    val dataSourceBuilder = DataSourceBuilder(env)
    val dataSource = dataSourceBuilder.getDataSource()

    val meldingDao = MeldingDao(dataSource)

    val producer = KafkaProducer(config.producerConfig(), StringSerializer(), StringSerializer())

    sendMeldinger(
        meldingDao,
        producer,
        topic
    )

    exitProcess(0)
}

internal fun sendMeldinger(
    meldingDao: MeldingDao,
    producer: KafkaProducer<String, String>,
    topic: String
) {
    val logger = LoggerFactory.getLogger("sparker")
    val startMillis = System.currentTimeMillis()

    val meldinger = meldingDao.hentMeldinger()
    meldinger.forEach {
        producer.send(createRecord(it, topic)).get()
    }

    producer.flush()
    producer.close()

    logger.info("Sendt ${meldinger.size} meldinger på ${(System.currentTimeMillis() - startMillis) / 1000}s")
}

private fun createRecord(fødselsnummer: String, topic: String): ProducerRecord<String, String> {
    val objectNode = objectMapper.createObjectNode()
    objectNode.put("@id", UUID.randomUUID().toString())
    objectNode.put("@event_name", "adressebeskyttelse_endret")
    objectNode.put("fødselsnummer", fødselsnummer)
    return ProducerRecord(topic, fødselsnummer, objectMapper.writeValueAsString(objectNode))
}
