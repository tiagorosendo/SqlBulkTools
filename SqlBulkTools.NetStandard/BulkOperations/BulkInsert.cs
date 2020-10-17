using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using SqlBulkTools.Enumeration;

// ReSharper disable UnusedMember.Global

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BulkInsert<T> : AbstractOperation<T>, ITransaction
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="bulk"></param>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="bulkCopySettings"></param>
        /// <param name="propertyInfoList"></param>
        public BulkInsert(BulkOperations bulk, IEnumerable<T> list, string tableName, string schema, HashSet<string> columns,
            Dictionary<string, string> customColumnMappings, BulkCopySettings bulkCopySettings, List<PropertyInfo> propertyInfoList) :
            base(bulk, list, tableName, schema, columns, customColumnMappings, bulkCopySettings, propertyInfoList)
        {
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkInsert<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
        {
            SetIdentity(columnName);
            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup.
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="outputIdentity"></param>
        /// <returns></returns>
        public BulkInsert<T> SetIdentityColumn(Expression<Func<T, object>> columnName, ColumnDirectionType outputIdentity)
        {
            SetIdentity(columnName, outputIdentity);
            return this;
        }

        /// <summary>
        /// Disables all Non-Clustered indexes on the table before the transaction and rebuilds after the
        /// transaction. This option should only be considered for very large operations.
        /// </summary>
        /// <returns></returns>
        public BulkInsert<T> TmpDisableAllNonClusteredIndexes()
        {
            _disableAllIndexes = true;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public int Commit(IDbConnection connection, IDbTransaction transaction = null)
        {
            if (connection is SqlConnection == false)
                throw new ArgumentException("Parameter must be a SqlConnection instance");

            return Commit((SqlConnection) connection, (SqlTransaction) transaction);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public BulkInsert<T> WithTimeout(int timeout)
        {
            _sqlTimeout = timeout;
            return this;
        }

        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for the operation to be
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public int Commit(SqlConnection connection, SqlTransaction transaction)
        {
            var affectedRows = 0;

            if (!_list.Any())
            {
                return affectedRows;
            }

            var dt = BulkOperationsHelper.CreateDataTable<T>(_propertyInfoList, _columns, _customColumnMappings, _ordinalDic, _matchTargetOn,
                _outputIdentity);
            dt = BulkOperationsHelper.ConvertListToDataTable(_propertyInfoList, dt, _list, _columns, _ordinalDic);

            // Must be after ToDataTable is called.
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);

            if (connection.State == ConnectionState.Closed)
                connection.Open();

            DataTable dtCols = null;
            if (_outputIdentity == ColumnDirectionType.InputOutput)
                dtCols = BulkOperationsHelper.GetDatabaseSchema(_bulk, connection, _schema, _tableName);

            //Bulk insert into temp table
            using var bulkCopy = new SqlBulkCopy(connection, _bulkCopySettings.SqlBulkCopyOptions, transaction)
            {
                DestinationTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema, _tableName)
            };

            BulkOperationsHelper.MapColumns(bulkCopy, _columns, _customColumnMappings);
            BulkOperationsHelper.SetSqlBulkCopySettings(bulkCopy, _bulkCopySettings);

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.CommandTimeout = _sqlTimeout;
            command.Transaction = transaction;

            if (_disableAllIndexes)
            {
                command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(Constants.Disable, _tableName,
                    _schema, connection);
                command.ExecuteNonQuery();
            }

            // If InputOutput identity is selected, must use staging table.
            if (_outputIdentity == ColumnDirectionType.InputOutput && dtCols != null)
            {
                command.CommandText = BulkOperationsHelper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
                command.ExecuteNonQuery();

                BulkOperationsHelper.InsertToTmpTable(connection, dt, _bulkCopySettings, transaction);

                command.CommandText = BulkOperationsHelper.GetInsertIntoStagingTableCmd(connection, _schema, _tableName,
                    _columns, _identityColumn, _outputIdentity);
                command.ExecuteNonQuery();

                BulkOperationsHelper.LoadFromTmpOutputTable(command, _identityColumn, _outputIdentityDic, OperationType.Insert, _list);
            }
            else
                bulkCopy.WriteToServer(dt);

            if (_disableAllIndexes)
            {
                command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(Constants.Rebuild, _tableName,
                    _schema, connection);
                command.ExecuteNonQuery();
            }

            bulkCopy.Close();

            affectedRows = dt.Rows.Count;
            return affectedRows;
        }

        /// <summary>
        /// Commits a transaction to database asynchronously. A valid setup must exist for the operation to be
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public async Task<int> CommitAsync(SqlConnection connection, SqlTransaction transaction)
        {
            var affectedRows = 0;

            if (!_list.Any())
            {
                return affectedRows;
            }

            var dt = BulkOperationsHelper.CreateDataTable<T>(_propertyInfoList, _columns, _customColumnMappings, _ordinalDic, _matchTargetOn,
                _outputIdentity);
            dt = BulkOperationsHelper.ConvertListToDataTable(_propertyInfoList, dt, _list, _columns, _ordinalDic);

            // Must be after ToDataTable is called.
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);

            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync();

            DataTable dtCols = null;
            if (_outputIdentity == ColumnDirectionType.InputOutput)
                dtCols = BulkOperationsHelper.GetDatabaseSchema(_bulk, connection, _schema, _tableName);

            using var bulkCopy = new SqlBulkCopy(connection, _bulkCopySettings.SqlBulkCopyOptions, transaction)
            {
                DestinationTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema, _tableName)
            };

            BulkOperationsHelper.MapColumns(bulkCopy, _columns, _customColumnMappings);
            BulkOperationsHelper.SetSqlBulkCopySettings(bulkCopy, _bulkCopySettings);

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.CommandTimeout = _sqlTimeout;
            command.Transaction = transaction;

            if (_disableAllIndexes)
            {
                command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(Constants.Disable, _tableName,
                    _schema, connection);
                await command.ExecuteNonQueryAsync();
            }

            // If InputOutput identity is selected, must use staging table.
            if (_outputIdentity == ColumnDirectionType.InputOutput && dtCols != null)
            {
                command.CommandText = BulkOperationsHelper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
                await command.ExecuteNonQueryAsync();

                BulkOperationsHelper.InsertToTmpTable(connection, dt, _bulkCopySettings, transaction);

                command.CommandText = BulkOperationsHelper.GetInsertIntoStagingTableCmd(connection, _schema, _tableName,
                    _columns, _identityColumn, _outputIdentity);
                await command.ExecuteNonQueryAsync();

                await BulkOperationsHelper.LoadFromTmpOutputTableAsync(command, _identityColumn, _outputIdentityDic, OperationType.Insert, _list);
            }
            else
                await bulkCopy.WriteToServerAsync(dt);

            if (_disableAllIndexes)
            {
                command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(Constants.Rebuild, _tableName,
                    _schema, connection);
                await command.ExecuteNonQueryAsync();
            }

            bulkCopy.Close();

            affectedRows = dt.Rows.Count;
            return affectedRows;
        }
    }
}