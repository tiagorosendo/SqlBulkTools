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
    public class BulkUpdate<T> : AbstractOperation<T>, ITransaction
    {
        private Dictionary<string, bool> _nullableColumnDic;

        /// <summary>
        /// Updates existing records in bulk.
        /// </summary>
        /// <param name="bulk"></param>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="bulkCopySettings"></param>
        /// <param name="propertyInfoList"></param>
        public BulkUpdate(BulkOperations bulk, IEnumerable<T> list, string tableName, string schema, HashSet<string> columns,
            Dictionary<string, string> customColumnMappings, BulkCopySettings bulkCopySettings, List<PropertyInfo> propertyInfoList)
            :
            base(bulk, list, tableName, schema, columns, customColumnMappings, bulkCopySettings, propertyInfoList)
        {
            _updatePredicates = new List<PredicateCondition>();
            _parameters = new List<SqlParameter>();
            _conditionSortOrder = 1;
            _nullableColumnDic = new Dictionary<string, bool>();
        }

        /// <summary>
        /// Only update records when the target satisfies a specific requirement. This is used in conjunction with MatchTargetOn.
        /// See help docs for examples.
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public BulkUpdate<T> UpdateWhen(Expression<Func<T, bool>> predicate)
        {
            BulkOperationsHelper.AddPredicate(predicate, PredicateType.Update, _updatePredicates, _parameters, _conditionSortOrder, Constants.UniqueParamIdentifier);
            _conditionSortOrder++;

            return this;
        }

        /// <summary>
        /// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating
        /// each row in table. This is usually set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed
        /// for matching composite relationships.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkUpdate<T> MatchTargetOn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new SqlBulkToolsException("MatchTargetOn column name can't be null.");

            _matchTargetOn.Add(propertyName);

            return this;
        }

        /// <summary>
        /// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating
        /// each row in table. This is usually set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed
        /// for matching composite relationships.
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="collation">Only explicitly set the collation if there is a collation conflict.</param>
        /// <returns></returns>
        public BulkUpdate<T> MatchTargetOn(Expression<Func<T, object>> columnName, string collation)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new NullReferenceException("MatchTargetOn column name can't be null.");

            _matchTargetOn.Add(propertyName);
            SetCollation(propertyName, collation);

            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkUpdate<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
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
        public BulkUpdate<T> SetIdentityColumn(Expression<Func<T, object>> columnName, ColumnDirectionType outputIdentity)
        {
            SetIdentity(columnName, outputIdentity);
            return this;
        }

        /// <summary>
        /// Sets the table hint to be used in the merge query. HOLDLOCK is the default that will be used if one is not set.
        /// </summary>
        /// <param name="tableHint"></param>
        /// <returns></returns>
        public BulkUpdate<T> SetTableHint(string tableHint)
        {
            _tableHint = tableHint;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public int Commit(IDbConnection connection, IDbTransaction transaction = null)
        {
            if (connection is SqlConnection == false)
                throw new ArgumentException("Parameter must be a SqlConnection instance");

            return Commit((SqlConnection)connection, (SqlTransaction)transaction);
        }

        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for the operation to be
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        /// <exception cref="IdentityException"></exception>
        public int Commit(SqlConnection connection, SqlTransaction transaction)
        {
            var affectedRows = 0;
            if (!_list.Any())
            {
                return affectedRows;
            }

            MatchTargetCheck();

            var dt = BulkOperationsHelper.CreateDataTable<T>(_propertyInfoList, _columns, _customColumnMappings, _ordinalDic, _matchTargetOn, _outputIdentity);
            dt = BulkOperationsHelper.ConvertListToDataTable(_propertyInfoList, dt, _list, _columns, _ordinalDic, _outputIdentityDic);

            // Must be after ToDataTable is called.
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _updatePredicates);

            if (connection.State == ConnectionState.Closed)
                connection.Open();

            var dtCols = BulkOperationsHelper.GetDatabaseSchema(_bulk, connection, _schema, _tableName);

            try
            {
                var command = connection.CreateCommand();
                command.Connection = connection;
                command.CommandTimeout = _sqlTimeout;
                command.Transaction = transaction;

                _nullableColumnDic = BulkOperationsHelper.GetNullableColumnDic(dtCols);

                //Creating temp table on database
                command.CommandText = BulkOperationsHelper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
                command.ExecuteNonQuery();

                //Bulk insert into temp table
                BulkOperationsHelper.InsertToTmpTable(connection, dt, _bulkCopySettings, transaction);

                var comm = BulkOperationsHelper.GetOutputCreateTableCmd(_outputIdentity, Constants.TempOutputTableName,
                OperationType.InsertOrUpdate, _identityColumn);

                if (!string.IsNullOrWhiteSpace(comm))
                {
                    command.CommandText = comm;
                    command.ExecuteNonQuery();
                }

                comm = GetCommand(connection);

                command.CommandText = comm;

                if (_parameters.Count > 0)
                {
                    command.Parameters.AddRange(_parameters.ToArray());
                }

                affectedRows = command.ExecuteNonQuery();

                if (_outputIdentity == ColumnDirectionType.InputOutput)
                {
                    BulkOperationsHelper.LoadFromTmpOutputTable(command, _identityColumn, _outputIdentityDic, OperationType.InsertOrUpdate, _list);
                }

                return affectedRows;
            }
            catch (SqlException e)
            {
                for (var i = 0; i < e.Errors.Count; i++)
                {
                    // Error 8102 is identity error.
                    if (e.Errors[i].Number == 8102)
                    {
                        // Expensive call but necessary to inform user of an important configuration setup.
                        throw new IdentityException(e.Errors[i].Message);
                    }
                }
                throw;
            }
        }

        /// <summary>
        /// Commits a transaction to database asynchronously. A valid setup must exist for the operation to be
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        /// <exception cref="IdentityException"></exception>
        public async Task<int> CommitAsync(SqlConnection connection, SqlTransaction transaction)
        {
            var affectedRows = 0;
            if (!_list.Any())
            {
                return affectedRows;
            }

            MatchTargetCheck();

            var dt = BulkOperationsHelper.CreateDataTable<T>(_propertyInfoList, _columns, _customColumnMappings, _ordinalDic, _matchTargetOn, _outputIdentity);
            dt = BulkOperationsHelper.ConvertListToDataTable(_propertyInfoList, dt, _list, _columns, _ordinalDic, _outputIdentityDic);

            // Must be after ToDataTable is called.
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _updatePredicates);

            if (connection.State == ConnectionState.Closed)
                await connection.OpenAsync();

            var dtCols = BulkOperationsHelper.GetDatabaseSchema(_bulk, connection, _schema, _tableName);

            try
            {
                var command = connection.CreateCommand();
                command.Connection = connection;
                command.CommandTimeout = _sqlTimeout;
                command.Transaction = transaction;

                _nullableColumnDic = BulkOperationsHelper.GetNullableColumnDic(dtCols);

                //Creating temp table on database
                command.CommandText = BulkOperationsHelper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
                await command.ExecuteNonQueryAsync();

                //Bulk insert into temp table
                BulkOperationsHelper.InsertToTmpTable(connection, dt, _bulkCopySettings, transaction);

                var comm = BulkOperationsHelper.GetOutputCreateTableCmd(_outputIdentity, Constants.TempOutputTableName,
                OperationType.InsertOrUpdate, _identityColumn);

                if (!string.IsNullOrWhiteSpace(comm))
                {
                    command.CommandText = comm;
                    await command.ExecuteNonQueryAsync();
                }

                comm = GetCommand(connection);

                command.CommandText = comm;

                if (_parameters.Count > 0)
                {
                    command.Parameters.AddRange(_parameters.ToArray());
                }

                affectedRows = await command.ExecuteNonQueryAsync();

                if (_outputIdentity == ColumnDirectionType.InputOutput)
                {
                    await BulkOperationsHelper.LoadFromTmpOutputTableAsync(command, _identityColumn, _outputIdentityDic, OperationType.InsertOrUpdate, _list);
                }

                return affectedRows;
            }
            catch (SqlException e)
            {
                for (var i = 0; i < e.Errors.Count; i++)
                {
                    // Error 8102 is identity error.
                    if (e.Errors[i].Number == 8102)
                    {
                        // Expensive call but necessary to inform user of an important configuration setup.
                        throw new IdentityException(e.Errors[i].Message);
                    }
                }
                throw;
            }
        }

        private string GetCommand(SqlConnection connection)
        {
            var comm = "MERGE INTO " + BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema, _tableName) + $" WITH ({_tableHint}) AS Target " +
                       "USING " + Constants.TempTableName + " AS Source " +
                       BulkOperationsHelper.BuildJoinConditionsForInsertOrUpdate(_matchTargetOn.ToArray(),
                           Constants.SourceAlias, Constants.TargetAlias, _collationColumnDic, _nullableColumnDic) +
                       "WHEN MATCHED " + BulkOperationsHelper.BuildPredicateQuery(_matchTargetOn.ToArray(), _updatePredicates, Constants.TargetAlias, _collationColumnDic) +
                       "THEN UPDATE " +
                       BulkOperationsHelper.BuildUpdateSet(_columns, Constants.SourceAlias, Constants.TargetAlias, _identityColumn) +
                       BulkOperationsHelper.GetOutputIdentityCmd(_identityColumn, _outputIdentity, Constants.TempOutputTableName,
                           OperationType.Update) + "; " +
                       "DROP TABLE " + Constants.TempTableName + ";";

            return comm;
        }
    }
}