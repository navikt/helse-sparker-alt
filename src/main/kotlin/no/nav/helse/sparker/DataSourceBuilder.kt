package no.nav.helse.sparker

import com.zaxxer.hikari.HikariConfig
import no.nav.vault.jdbc.hikaricp.HikariCPVaultUtil.createHikariDataSourceWithVaultIntegration as createDataSource


internal class DataSourceBuilder(private val env: Map<String, String>) {
    private val hikariConfig = HikariConfig().apply {
        jdbcUrl = env["DATABASE_JDBC_URL"]
        maximumPoolSize = 3
        minimumIdle = 1
        idleTimeout = 10001
        connectionTimeout = 1000
        maxLifetime = 30001
    }

    private val databaseName = env["DATABASE_NAME"]
    private val vaultMountPath = env["VAULT_MOUNTPATH"]

    fun getDataSource() = createDataSource(hikariConfig, vaultMountPath, "$databaseName-user")
}
