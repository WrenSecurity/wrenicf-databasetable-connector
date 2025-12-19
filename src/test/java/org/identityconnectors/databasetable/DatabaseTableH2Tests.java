/*
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2008-2009 Sun Microsystems, Inc. All rights reserved.
 *
 * The contents of this file are subject to the terms of the Common Development
 * and Distribution License("CDDL") (the "License").  You may not use this file
 * except in compliance with the License.
 *
 * You can obtain a copy of the License at
 * http://IdentityConnectors.dev.java.net/legal/license.txt
 * See the License for the specific language governing permissions and limitations
 * under the License.
 *
 * When distributing the Covered Code, include this CDDL Header Notice in each file
 * and include the License file at identityconnectors/legal/license.txt.
 * If applicable, add the following below this CDDL Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 * Portions Copyrighted 2015 ForgeRock AS
 * Portions Copyright 2025 Wren Security.
 */
package org.identityconnectors.databasetable;

import static org.identityconnectors.common.ByteUtil.randomBytes;
import static org.identityconnectors.common.StringUtil.randomString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.h2.tools.RunScript;
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.IOUtil;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.databasetable.mapping.MappingStrategy;
import org.identityconnectors.dbcommon.ExpectProxy;
import org.identityconnectors.dbcommon.SQLParam;
import org.identityconnectors.dbcommon.SQLUtil;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Attempts to test the Connector with the framework.
 */
public class DatabaseTableH2Tests extends DatabaseTableTestBase {

    public static final String H2_DRIVER = "org.h2.Driver";
    public static final String H2_DATABASE = "testdb";
    public static final String H2_JDBC_URL = "jdbc:h2:mem:" + H2_DATABASE;

    public static final String SHUTDOWN = "SHUTDOWN";

    // The tested table
    private static final String DB_TABLE = "Accounts";

    private static Connection connection;

    // Setup/Teardown
    /**
     * Creates a temporary database based on a SQL resource file.
     *
     * @throws Exception
     */
    @BeforeAll
    public static void createDatabase() throws Exception {
        Class.forName(H2_DRIVER);
        connection = DriverManager.getConnection(H2_JDBC_URL);
        InputStream sqlScript = DatabaseTableH2Tests.class.getClassLoader().getResourceAsStream("h2.sql");
        RunScript.execute(connection, new InputStreamReader(sqlScript));
    }

    @AfterAll
    public static void deleteDatabase() throws Exception {
        connection.createStatement().execute(SHUTDOWN);
        connection = null;
    }

    /**
     * Create the test configuration
     *
     * @return the initialized configuration
     */
    @Override
    protected DatabaseTableConfiguration getConfiguration() throws Exception {
        DatabaseTableConfiguration config = new DatabaseTableConfiguration();
        config.setJdbcDriver(H2_DRIVER);
        config.setUser("");
        config.setPassword(new GuardedString("".toCharArray()));
        config.setTable(DB_TABLE);
        config.setKeyColumn(ACCOUNTID);
        config.setPasswordColumn(PASSWORD);
        config.setDatabase(H2_DATABASE);
        config.setJdbcUrlTemplate(H2_JDBC_URL);
        config.setChangeLogColumn(CHANGELOG);
        config.setConnectorMessages(TestHelpers.createDummyMessages());
        return config;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.identityconnectors.databasetable.DatabaseTableTestBase#
     * getCreateAttributeSet()
     */
    @Override
    protected Set<Attribute> getCreateAttributeSet(DatabaseTableConfiguration cfg) throws Exception {
        Set<Attribute> ret = new HashSet<Attribute>();
        ret.add(AttributeBuilder.build(Name.NAME, randomString(r, 50)));
        if (StringUtil.isNotBlank(cfg.getPasswordColumn())) {
            ret.add(AttributeBuilder.buildPassword(new GuardedString(randomString(r, 50).toCharArray())));
        } else {
            ret.add(AttributeBuilder.build(PASSWORD, randomString(r, 40)));
        }
        ret.add(AttributeBuilder.build(MANAGER, randomString(r, 15)));
        ret.add(AttributeBuilder.build(MIDDLENAME, randomString(r, 50)));
        ret.add(AttributeBuilder.build(FIRSTNAME, randomString(r, 50)));
        ret.add(AttributeBuilder.build(LASTNAME, randomString(r, 50)));
        ret.add(AttributeBuilder.build(EMAIL, randomString(r, 50)));
        ret.add(AttributeBuilder.build(DEPARTMENT, randomString(r, 50)));
        ret.add(AttributeBuilder.build(TITLE, randomString(r, 50)));
        if (!cfg.getChangeLogColumn().equalsIgnoreCase(AGE)) {
            ret.add(AttributeBuilder.build(AGE, r.nextInt(100)));
        }
        if (!cfg.getChangeLogColumn().equalsIgnoreCase(ACCESSED)) {
            ret.add(AttributeBuilder.build(ACCESSED, r.nextLong()));
        }
        ret.add(AttributeBuilder.build(SALARY, new BigDecimal("360536.75")));
        ret.add(AttributeBuilder.build(JPEGPHOTO, randomBytes(r, 2000)));
        ret.add(AttributeBuilder.build(OPENTIME, new java.sql.Time(System.currentTimeMillis()).toString()));
        ret.add(AttributeBuilder.build(ACTIVATE, new java.sql.Date(System.currentTimeMillis()).toString()));
        ret.add(AttributeBuilder.build(ENROLLED, new Timestamp(System.currentTimeMillis()).toString()));
        ret.add(AttributeBuilder.build(CHANGED, new Timestamp(System.currentTimeMillis()).toString()));
        if (!cfg.getChangeLogColumn().equalsIgnoreCase(CHANGELOG)) {
            ret.add(AttributeBuilder.build(CHANGELOG, new Timestamp(System.currentTimeMillis()).getTime()));
        }
        return ret;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.identityconnectors.databasetable.DatabaseTableTestBase#
     * getModifyAttributeSet()
     */
    @Override
    protected Set<Attribute> getModifyAttributeSet(DatabaseTableConfiguration cfg) throws Exception {
        return getCreateAttributeSet(cfg);
    }

    /**
     * test method
     */
    @Override
    @Test
    public void testConfiguration() {
        // attempt to test driver info..
        DatabaseTableConfiguration config = new DatabaseTableConfiguration();
        // check defaults..
        config.setJdbcDriver(H2_DRIVER);
        assertEquals(H2_DRIVER, config.getJdbcDriver());
        config.setKeyColumn(ACCOUNTID);
        assertEquals(ACCOUNTID, config.getKeyColumn());
        config.setTable(DB_TABLE);
        assertEquals(DB_TABLE, config.getTable());
        config.setJdbcUrlTemplate(H2_JDBC_URL);
        assertEquals(H2_JDBC_URL, config.getJdbcUrlTemplate());
        config.setDatabase(H2_DATABASE);
        assertEquals(H2_DATABASE, config.getDatabase());
        config.setUser(ACCOUNTID);
        assertEquals(ACCOUNTID, config.getUser());
        config.setPassword(new GuardedString("".toCharArray()));
        assertEquals(ACCOUNTID, config.getUser());
        config.validate();
    }

    /**
     * For testing purposes we creating connection an not the framework.
     *
     * @throws Exception
     */
    @Test
    public void testNoZeroSQLExceptions() throws Exception {
        DatabaseTableConfiguration cfg = getConfiguration();
        cfg.setRethrowAllSQLExceptions(false);
        con = getConnector(cfg);

        final ExpectProxy<MappingStrategy> smse = new ExpectProxy<MappingStrategy>();
        MappingStrategy sms = smse.getProxy(MappingStrategy.class);
        // Schema
        for (int i = 0; i < 15; i++) {
            smse.expectAndReturn("getSQLAttributeType", String.class);
        }
        // Create fail
        smse.expectAndThrow("setSQLParam", new SQLException("test reason", "0", 0));
        // Update fail
        smse.expectAndThrow("setSQLParam", new SQLException("test reason", "0", 0));
        con.getConn().setSms(sms);
        Set<Attribute> expected = getCreateAttributeSet(cfg);
        Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        con.update(ObjectClass.ACCOUNT, uid, expected, null);
        assertTrue(smse.isDone(), "setSQLParam not called");
    }

    /**
     * For testing purposes we creating connection an not the framework.
     *
     * @throws Exception
     */
    @Test
    public void testNonZeroSQLExceptions() throws Exception {
        DatabaseTableConfiguration cfg = getConfiguration();
        cfg.setRethrowAllSQLExceptions(false);
        con = getConnector(cfg);

        final ExpectProxy<MappingStrategy> smse = new ExpectProxy<MappingStrategy>();
        MappingStrategy sms = smse.getProxy(MappingStrategy.class);
        for (int i = 0; i < 15; i++) {
            smse.expectAndReturn("getSQLAttributeType", String.class);
        }
        smse.expectAndThrow("setSQLParam", new SQLException("test reason", "411", 411));
        con.getConn().setSms(sms);
        Set<Attribute> expected = getCreateAttributeSet(cfg);
        assertThrows(ConnectorException.class, () -> {
            con.create(ObjectClass.ACCOUNT, expected, null);
        });
        assertTrue(smse.isDone(), "setSQLParam not called");
    }

    /**
     * For testing purposes we creating connection an not the framework.
     *
     * @throws Exception
     */
    @Test
    public void testRethrowAllSQLExceptions() throws Exception {
        DatabaseTableConfiguration cfg = getConfiguration();
        cfg.setRethrowAllSQLExceptions(true);
        con = getConnector(cfg);

        final ExpectProxy<MappingStrategy> smse = new ExpectProxy<MappingStrategy>();
        MappingStrategy sms = smse.getProxy(MappingStrategy.class);
        for (int i = 0; i < 15; i++) {
            smse.expectAndReturn("getSQLAttributeType", String.class);
        }
        smse.expectAndThrow("setSQLParam", new SQLException("test reason", "0", 0));
        con.getConn().setSms(sms);
        Set<Attribute> expected = getCreateAttributeSet(cfg);
        assertThrows(ConnectorException.class, () -> {
            con.create(ObjectClass.ACCOUNT, expected, null);
        });
        assertTrue(smse.isDone(), "setSQLParam not called");
    }

    /**
     * For testing purposes we creating connection an not the framework.
     *
     * @throws Exception
     */
    @Test
    public void testSchema() throws Exception {
        DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        // check if this works..
        Schema schema = con.schema();
        checkSchema(schema, cfg);

        // Negative test
        cfg.setSuppressPassword(!cfg.getSuppressPassword());
        con = getConnector(cfg);
        // check if this works..
        schema = con.schema();
        checkSchema(schema, cfg);
    }

    /**
     * check validity of the schema
     *
     * @param schema the schema to be checked
     * @throws Exception
     */
    void checkSchema(Schema schema, DatabaseTableConfiguration cfg) throws Exception {
        // Schema should not be null
        assertNotNull(schema);
        Set<ObjectClassInfo> objectInfos = schema.getObjectClassInfo();
        assertNotNull(objectInfos);
        assertEquals(1, objectInfos.size());
        // get the fields from the test account
        final Set<Attribute> attributeSet = getCreateAttributeSet(cfg);
        final Map<String, Attribute> expected = AttributeUtil.toMap(attributeSet);
        final Set<String> keys = CollectionUtil.newCaseInsensitiveSet();
        keys.addAll(expected.keySet());

        // iterate through ObjectClassInfo Set
        for (ObjectClassInfo objectInfo : objectInfos) {
            assertNotNull(objectInfo);
            // the object class has to ACCOUNT_NAME
            assertTrue(objectInfo.is(ObjectClass.ACCOUNT_NAME));

            // iterate through AttributeInfo Set
            for (AttributeInfo attInfo : objectInfo.getAttributeInfo()) {
                assertNotNull(attInfo);
                String fieldName = attInfo.getName();
                if (fieldName.equalsIgnoreCase(CHANGELOG)) {
                    keys.remove(fieldName);
                    continue;
                }
                if (fieldName.equalsIgnoreCase(OperationalAttributes.PASSWORD_NAME)) {
                    assertEquals(cfg.getSuppressPassword(), !attInfo.isReadable());
                }
                assertTrue(keys.contains(fieldName), "Field:" + fieldName + " doesn't exist");
                keys.remove(fieldName);
                Attribute fa = expected.get(fieldName);
                assertNotNull(fa, "Field:" + fieldName + "  was duplicated");
                Object field = AttributeUtil.getSingleValue(fa);
                Class<?> valueClass = field.getClass();
                assertEquals(valueClass, attInfo.getType(), "field: " + fieldName);
            }
            // all the attribute has to be removed
            assertEquals(0, keys.size(), "There are missing attributes which were not included in the schema:" + keys);
        }
    }

    /**
     * Test creating of the connector object, searching using UID and delete
     *
     * @throws Exception
     * @throws SQLException
     */
    @Test
    public void testGetLatestSyncToken() throws Exception {
        final String SQL_TEMPLATE = "UPDATE Accounts SET changelog = ? WHERE accountId = ?";
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        deleteAllFromAccounts(con.getConn());
        final Set<Attribute> expected = getCreateAttributeSet(cfg);

        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        assertNotNull(uid);
        final Long changelog = 9999999999999L; // Some really big value

        // update the last change
        PreparedStatement ps = null;
        DatabaseTableConnection conn = null;
        try {
            conn = DatabaseTableConnection.createDBTableConnection(getConfiguration());

            List<SQLParam> values = new ArrayList<SQLParam>();
            values.add(new SQLParam("changelog", changelog, Types.INTEGER));
            values.add(new SQLParam("accountId", uid.getUidValue(), Types.VARCHAR));
            ps = conn.prepareStatement(SQL_TEMPLATE, values);
            ps.execute();
            conn.commit();
        } finally {
            IOUtil.quietClose(ps);
            SQLUtil.closeQuietly(conn);
        }
        // attempt to find the newly created object..
        final SyncToken latestSyncToken = con.getLatestSyncToken(ObjectClass.ACCOUNT);
        assertNotNull(latestSyncToken);
        final Object actual = latestSyncToken.getValue();
        assertEquals(changelog, actual);
    }

}
