package org.softlab.dataset.jdbc.sqlite

import org.dbunit.dataset.datatype.DataType
import java.sql.Types
import kotlin.test.Test
import kotlin.test.assertSame


class SqliteDataTypeFactoryTest {
    @Test
    fun `createDataType() maps SQLite storage types`() {
        val cut = SqliteDataTypeFactory()
        assertSame(DataType.BLOB, cut.createDataType(Types.OTHER, "BloB"))
        assertSame(DataType.BOOLEAN, cut.createDataType(Types.OTHER, "BooLean"))
    }

    @Test
    fun `createDataType() falls back to default DBUnit type factory`() {
        val cut = SqliteDataTypeFactory()

        assertSame(DataType.INTEGER, cut.createDataType(Types.INTEGER, "integer"))
    }
}
