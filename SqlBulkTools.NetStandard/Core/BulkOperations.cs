using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
// ReSharper disable UnusedMember.Global

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class BulkOperations : IBulkOperations
    {
        /// <summary>
        /// Each transaction requires a valid setup. Examples available at: https://github.com/gtaylor44/SqlBulkTools 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public Setup<T> Setup<T>() where T : class
        {
            if (typeof(T) == typeof(ExpandoObject))
                throw new ArgumentException("ExpandoObject is currently not supported.");

            return new Setup<T>(this);
        }

        /// <summary>
        /// Each transaction requires a valid setup. Examples available at: https://github.com/gtaylor44/SqlBulkTools 
        /// </summary>
        /// <returns></returns>
        public Setup Setup()
        {
            return new Setup(this);
        }


        /// <summary>
        /// Utility to prefetch schema information meta data for a given SQL table.
        /// Necessary when using Transaction around Bulk Operations.
        /// </summary>
        public void Prepare(SqlConnection conn, string tableName)
        {
            var table = BulkOperationsHelper.GetTableAndSchema(tableName);
            Prepare(conn, table.Schema, table.Name);
        }
        internal DataTable Prepare(SqlConnection conn, string schema, string tableName)
        {
            var sk = new SchemaKey(conn.Database, schema, tableName);
            if (_schemaCache.TryGetValue(sk, out var result))
                return result;

            if (conn.State != ConnectionState.Open)
                conn.Open();

            var dtCols = conn.GetSchema("Columns", sk.ToRestrictions());

            if (dtCols.Rows.Count == 0 && schema != null)
                throw new SqlBulkToolsException(
                    $"Table name '{tableName}' with schema name '{schema}' not found. Check your setup and try again.");
            if (dtCols.Rows.Count == 0)
            {
                throw new SqlBulkToolsException($"Table name '{tableName}' not found. Check your setup and try again.");
            }

            _schemaCache[sk] = dtCols;
            return dtCols;
        }

        class SchemaKey
        {
            private readonly string _database, _schema, _tableName;
            public SchemaKey(string database, string schema, string tableName)
            {
                _database = database;
                _schema = schema;
                _tableName = tableName;
            }

            public string[] ToRestrictions() => new[]
            {
                _database,
                _schema,
                _tableName,
                null
            };

            public override int GetHashCode()
            {
                return _database.GetHashCode() ^ _schema.GetHashCode() ^ _tableName.GetHashCode();
            }
            public override bool Equals(object obj)
            {
                return obj is SchemaKey sk
                    && sk._database == _database
                    && sk._schema == _schema
                    && sk._tableName == _tableName;
            }
        }

        private readonly Dictionary<SchemaKey, DataTable> _schemaCache = new Dictionary<SchemaKey, DataTable>();
    }
}