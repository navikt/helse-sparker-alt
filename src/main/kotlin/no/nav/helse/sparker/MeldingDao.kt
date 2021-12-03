package no.nav.helse.sparker

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import org.intellij.lang.annotations.Language
import javax.sql.DataSource

class MeldingDao(private val dataSource: DataSource) {
    fun hentMeldinger() = using(sessionOf(dataSource)) { session ->
        @Language("PostgreSQL")
        val query = """SELECT fodselsnummer FROM person;"""
        session.run(queryOf(query).map {
            it.long("fodselsnummer").toString().padStart(11, '0')
        }.asList)
    }
}
