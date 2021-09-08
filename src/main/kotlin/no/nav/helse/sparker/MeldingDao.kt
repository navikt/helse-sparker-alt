package no.nav.helse.sparker

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import org.intellij.lang.annotations.Language
import javax.sql.DataSource

class MeldingDao(private val dataSource: DataSource) {
    fun hentMeldinger(meldingTypeId: Long) = using(sessionOf(dataSource)) { session ->
        @Language("PostgreSQL")
        val query = """SELECT (melding.json #>>'{}') as json FROM melding WHERE melding_type_id=?;"""
        session.run(queryOf(query, meldingTypeId).map {
            it.string("json")
        }.asList)
    }
}
