package no.nav.helse.sparker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
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
    val eventName = env.getValue("EVENT_NAME")
    val meldingTypeId = env.getValue("MELDING_TYPE_ID").toLong()

    val dataSourceBuilder = DataSourceBuilder(env)
    val dataSource = dataSourceBuilder.getDataSource()

    val meldingDao = MeldingDao(dataSource)

    val producer = KafkaProducer(config.producerConfig(), StringSerializer(), StringSerializer())

    sendMeldinger(
        meldingDao,
        producer,
        meldingTypeId,
        eventName,
        topic
    )

    exitProcess(0)
}

internal fun sendMeldinger(
    meldingDao: MeldingDao,
    producer: KafkaProducer<String, String>,
    meldingTypeId: Long,
    eventName: String,
    topic: String
) {
    val logger = LoggerFactory.getLogger("sparker")
    val startMillis = System.currentTimeMillis()

    val meldinger = meldingDao.hentMeldinger(meldingTypeId)
    meldinger.forEach {
        producer.send(createRecord(it, topic, eventName)).get()
    }

    producer.flush()
    producer.close()

    logger.info("Sendt ${meldinger.size} meldinger på ${(System.currentTimeMillis() - startMillis) / 1000}s for melding id $meldingTypeId")
}

private fun createRecord(input: String, topic: String, eventName: String): ProducerRecord<String, String> {
    val objectNode = objectMapper.readValue<ObjectNode>(input)
    objectNode.put("@event_name", eventName)
    return ProducerRecord(topic, objectNode.get("fødselsnummer").asText(), objectMapper.writeValueAsString(objectNode))
}
