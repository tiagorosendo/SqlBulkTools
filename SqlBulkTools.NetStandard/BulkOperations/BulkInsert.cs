using SqlBulkTools.Enumeration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

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
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="bulkCopySettings"></param>
        /// <param name="propertyInfoList"></param>
        public BulkInsert(BulkOperations bulk, IEnumerable<T> list, string tableName, string schema, HashSet<string> columns,
            Dictionary<string, string> customColumnMappings, BulkCopySettings bulkCopySettings, List<PropInfo> propertyInfoList) :

            base(bulk, list, tableName, schema, columns, customColumnMappings, bulkCopySettings, propertyInfoList)
        { }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkInsert<T> SetIdentityColumn(string columnName)
        {
            base.SetIdentity(columnName);
            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkInsert<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
        {
            base.SetIdentity(columnName);
            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup.
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="outputIdentity"></param>
        /// <returns></returns>
        public BulkInsert<T> SetIdentityColumn(string columnName, ColumnDirectionType outputIdentity)
        {
            base.SetIdentity(columnName, outputIdentity);
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
            base.SetIdentity(columnName, outputIdentity);
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

        public int Commit(IDbConnection connection, IDbTransaction transaction = null)
        {
            if (connection is SqlConnection == false)
                throw new ArgumentException("Parameter must be a SqlConnection instance");

            return Commit((SqlConnection)connection, (SqlTransaction)transaction);
        }

        public Task<int> CommitAsync(IDbConnection connection, IDbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            if (connection is SqlConnection == false)
                throw new ArgumentException("Parameter must be a SqlConnection instance");

            return CommitAsync((SqlConnection)connection, (SqlTransaction)transaction, cancellationToken);
        }

        public BulkInsert<T> WithTimeout(int timeout)
        {
            this._sqlTimeout = timeout;
            return this;
        }

        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for the operation to be
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public int Commit(SqlConnection connection, SqlTransaction transaction)
        {
            int affectedRows = 0;

            if (!_list.Any())
            {
                return affectedRows;
            }

            DataTable dt = BulkOperationsHelper.CreateDataTable<T>(_propertyInfoList, _columns, _customColumnMappings, _ordinalDic, _matchTargetOn, _outputIdentity);
            dt = BulkOperationsHelper.ConvertListToDataTable(_propertyInfoList, dt, _list, _columns, _ordinalDic);

            // Must be after ToDataTable is called.
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);

            if (connection.State == ConnectionState.Closed)
                connection.Open();

            DataTable dtCols = null;
            if (_outputIdentity == ColumnDirectionType.InputOutput)
                dtCols = BulkOperationsHelper.GetDatabaseSchema(bulk, connection, _schema, _tableName);

            //Bulk insert into temp table
            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connection, _bulkCopySettings.SqlBulkCopyOptions, transaction))
            {
                bulkcopy.DestinationTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema, _tableName);
                BulkOperationsHelper.MapColumns(bulkcopy, _columns, _customColumnMappings);

                BulkOperationsHelper.SetSqlBulkCopySettings(bulkcopy, _bulkCopySettings);

                SqlCommand command = connection.CreateCommand();
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

                    BulkOperationsHelper.LoadFromTmpOutputTable(command, _propertyInfoList, _identityColumn, _outputIdentityDic, OperationType.Insert, _list);
                }
                else
                    bulkcopy.WriteToServer(dt);

                if (_disableAllIndexes)
                {
                    command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(Constants.Rebuild, _tableName,
                        _schema, connection);
                    command.ExecuteNonQuery();
                }

                bulkcopy.Close();

                affectedRows = dt.Rows.Count;
                return affectedRows;
            }
        }

        /// <summary>
        /// Commits a transaction to database asynchronously. A valid setup must exist for the operation to be
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public async Task<int> CommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken cancellationToken)
        {
            int affectedRows = 0;

            if (!_list.Any())
            {
                return affectedRows;
            }

            DataTable dt = BulkOperationsHelper.CreateDataTable<T>(_propertyInfoList, _columns, _customColumnMappings, _ordinalDic, _matchTargetOn, _outputIdentity);
            dt = BulkOperationsHelper.ConvertListToDataTable(_propertyInfoList, dt, _list, _columns, _ordinalDic);

            // Must be after ToDataTable is called.
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);

            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            DataTable dtCols = null;
            if (_outputIdentity == ColumnDirectionType.InputOutput)
                dtCols = BulkOperationsHelper.GetDatabaseSchema(bulk, connection, _schema, _tableName);

            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connection, _bulkCopySettings.SqlBulkCopyOptions, transaction))
            {
                bulkcopy.DestinationTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema, _tableName);
                BulkOperationsHelper.MapColumns(bulkcopy, _columns, _customColumnMappings);

                BulkOperationsHelper.SetSqlBulkCopySettings(bulkcopy, _bulkCopySettings);

                SqlCommand command = connection.CreateCommand();
                command.Connection = connection;
                command.CommandTimeout = _sqlTimeout;
                command.Transaction = transaction;

                if (_disableAllIndexes)
                {
                    command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(Constants.Disable, _tableName,
                        _schema, connection);
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                // If InputOutput identity is selected, must use staging table.
                if (_outputIdentity == ColumnDirectionType.InputOutput && dtCols != null)
                {
                    command.CommandText = BulkOperationsHelper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                    await BulkOperationsHelper.InsertToTmpTableAsync(connection, dt, _bulkCopySettings, transaction, cancellationToken).ConfigureAwait(false);

                    command.CommandText = BulkOperationsHelper.GetInsertIntoStagingTableCmd(connection, _schema, _tableName,
                        _columns, _identityColumn, _outputIdentity);
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                    await BulkOperationsHelper.LoadFromTmpOutputTableAsync(command, _propertyInfoList, _identityColumn, _outputIdentityDic, OperationType.Insert, _list, cancellationToken).ConfigureAwait(false);
                }
                else
                    await bulkcopy.WriteToServerAsync(dt, cancellationToken).ConfigureAwait(false);

                if (_disableAllIndexes)
                {
                    command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(Constants.Rebuild, _tableName,
                        _schema, connection);
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                bulkcopy.Close();

                affectedRows = dt.Rows.Count;
                return affectedRows;
            }
        }
    }
}
