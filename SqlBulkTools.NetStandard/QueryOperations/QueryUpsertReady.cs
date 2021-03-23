using SqlBulkTools.Enumeration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class QueryUpsertReady<T> : ITransaction
    {
        private readonly T _singleEntity;
        private readonly string _tableName;
        private readonly string _schema;
        private readonly HashSet<string> _columns;
        private readonly Dictionary<string, string> _customColumnMappings;
        private string _identityColumn;
        private readonly List<SqlParameter> _sqlParams;
        private readonly HashSet<string> _matchTargetOn;
        private readonly HashSet<string> _excludeFromUpdate;
        private ColumnDirectionType _outputIdentity;
        private readonly Dictionary<string, string> _collationColumnDic;
        private readonly List<PropInfo> _propertyInfoList;

        /// <summary>
        ///
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="sqlParams"></param>
        /// <param name="propertyInfoList"></param>
        public QueryUpsertReady(T singleEntity, string tableName, string schema, HashSet<string> columns, Dictionary<string, string> customColumnMappings,
            List<SqlParameter> sqlParams, List<PropInfo> propertyInfoList)
        {
            _singleEntity = singleEntity;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _customColumnMappings = customColumnMappings;
            _sqlParams = sqlParams;
            _matchTargetOn = new HashSet<string>();
            _outputIdentity = ColumnDirectionType.Input;
            _excludeFromUpdate = new HashSet<string>();
            _collationColumnDic = new Dictionary<string, string>();
            _propertyInfoList = propertyInfoList;
        }

        /// <summary>
        /// Sets the identity column for the table.
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="outputIdentity"></param>
        /// <returns></returns>
        public QueryUpsertReady<T> SetIdentityColumn(Expression<Func<T, object>> columnName, ColumnDirectionType outputIdentity)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            _outputIdentity = outputIdentity;

            if (propertyName == null)
                throw new SqlBulkToolsException("SetIdentityColumn column name can't be null");

            if (_identityColumn == null)
                _identityColumn = BulkOperationsHelper.GetActualColumn(_customColumnMappings, propertyName);
            else
                throw new SqlBulkToolsException("Can't have more than one identity column");

            return this;
        }

        /// <summary>
        /// Exclude a property from the update statement. Useful for when you want to include CreatedDate or Guid for inserts only.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public QueryUpsertReady<T> ExcludeColumnFromUpdate(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new SqlBulkToolsException("ExcludeColumnFromUpdate column name can't be null");

            if (!_columns.Contains(propertyName))
            {
                throw new SqlBulkToolsException("ExcludeColumnFromUpdate could not exclude column from update because column could not " +
                                                "be recognised. Call AddAllColumns() or AddColumn() for this column first.");
            }
            _excludeFromUpdate.Add(propertyName);

            return this;
        }

        /// <summary>
        /// Sets the identity column for the table.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public QueryUpsertReady<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new SqlBulkToolsException("SetIdentityColumn column name can't be null");

            if (_identityColumn == null)
                _identityColumn = BulkOperationsHelper.GetActualColumn(_customColumnMappings, propertyName);
            else
                throw new SqlBulkToolsException("Can't have more than one identity column");

            return this;
        }

        /// <summary>
        /// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating
        /// each row in table. This is usally set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed
        /// for matching composite relationships.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public QueryUpsertReady<T> MatchTargetOn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new NullReferenceException("MatchTargetOn column name can't be null.");

            _matchTargetOn.Add(propertyName);

            return this;
        }

        /// <summary>
        /// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating
        /// each row in table. This is usally set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed
        /// for matching composite relationships.
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="collation">Only explicitly set the collation if there is a collation conflict.</param>
        /// <returns></returns>
        public QueryUpsertReady<T> MatchTargetOn(Expression<Func<T, object>> columnName, string collation)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new NullReferenceException("MatchTargetOn column name can't be null.");

            _matchTargetOn.Add(propertyName);

            if (collation == null)
                throw new SqlBulkToolsException("Collation can't be null");

            _collationColumnDic.Add(BulkOperationsHelper.GetActualColumn(_customColumnMappings, propertyName), collation);

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

        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for the operation to be
        /// successful.
        /// </summary>
        /// <param name="conn"></param>
        /// <returns></returns>
        /// <exception cref="NullReferenceException"></exception>
        /// <exception cref="IdentityException"></exception>
        public int Commit(SqlConnection conn, SqlTransaction transaction)
        {
            int affectedRows = 0;
            if (_singleEntity == null)
            {
                return affectedRows;
            }

            if (_matchTargetOn.Count == 0)
                throw new NullReferenceException("MatchTargetOn is a mandatory for upsert operation");

            try
            {
                if (conn.State == ConnectionState.Closed)
                    conn.Open();

                BulkOperationsHelper.AddSqlParamsForQuery(_propertyInfoList, _sqlParams, _columns, _singleEntity, _identityColumn, _outputIdentity, _customColumnMappings);
                BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns);

                SqlCommand command = conn.CreateCommand();
                command.Connection = conn;
                command.Transaction = transaction;

                string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(conn.Database, _schema, _tableName);

                command.CommandText = GetCommand(fullQualifiedTableName);

                if (_sqlParams.Count > 0)
                {
                    command.Parameters.AddRange(_sqlParams.ToArray());
                }

                affectedRows = command.ExecuteNonQuery();

                if (_outputIdentity == ColumnDirectionType.InputOutput)
                {
                    foreach (var x in _sqlParams)
                    {
                        if (x.Direction == ParameterDirection.InputOutput
                            && x.ParameterName == $"@{_identityColumn}")
                        {
                            if (x.Value is DBNull)
                            {
                                break;
                            }
                            PropertyInfo propertyInfo = _singleEntity.GetType().GetProperty(_identityColumn);
                            propertyInfo.SetValue(_singleEntity, x.Value);
                            break;
                        }
                    }
                }

                return affectedRows;
            }
            catch (SqlException e)
            {
                for (int i = 0; i < e.Errors.Count; i++)
                {
                    // Error 8102 and 544 is identity error.
                    if (e.Errors[i].Number == 544 || e.Errors[i].Number == 8102)
                    {
                        // Expensive but neccessary to inform user of an important configuration setup.
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
        /// <param name="conn"></param>
        /// <returns></returns>
        /// <exception cref="NullReferenceException"></exception>
        /// <exception cref="IdentityException"></exception>
        public async Task<int> CommitAsync(SqlConnection conn, SqlTransaction transaction, CancellationToken cancellationToken)
        {
            int affectedRows = 0;
            if (_singleEntity == null)
            {
                return affectedRows;
            }

            if (_matchTargetOn.Count == 0)
                throw new NullReferenceException("MatchTargetOn is a mandatory for upsert operation");

            try
            {
                BulkOperationsHelper.AddSqlParamsForQuery(_propertyInfoList, _sqlParams, _columns, _singleEntity, _identityColumn, _outputIdentity, _customColumnMappings);
                BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns);

                if (conn.State == ConnectionState.Closed)
                    await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

                SqlCommand command = conn.CreateCommand();
                command.Connection = conn;
                command.Transaction = transaction;

                string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(conn.Database, _schema, _tableName);

                command.CommandText = GetCommand(fullQualifiedTableName);

                if (_sqlParams.Count > 0)
                {
                    command.Parameters.AddRange(_sqlParams.ToArray());
                }

                affectedRows = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                if (_outputIdentity == ColumnDirectionType.InputOutput)
                {
                    foreach (var x in _sqlParams)
                    {
                        if (x.Direction == ParameterDirection.InputOutput
                            && x.ParameterName == $"@{_identityColumn}")
                        {
                            if (x.Value is DBNull)
                            {
                                break;
                            }

                            PropertyInfo propertyInfo = _singleEntity.GetType().GetProperty(_identityColumn);
                            propertyInfo.SetValue(_singleEntity, x.Value);
                            break;
                        }
                    }
                }

                return affectedRows;
            }
            catch (SqlException e)
            {
                for (int i = 0; i < e.Errors.Count; i++)
                {
                    // Error 8102 and 544 is identity error.
                    if (e.Errors[i].Number == 544 || e.Errors[i].Number == 8102)
                    {
                        // Expensive but neccessary to inform user of an important configuration setup.
                        throw new IdentityException(e.Errors[i].Message);
                    }
                }

                throw;
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="fullQualifiedTableName"></param>
        /// <returns></returns>
        public string GetCommand(string fullQualifiedTableName)
        {
            return $"UPDATE {fullQualifiedTableName} {BulkOperationsHelper.BuildUpdateSet(_columns, _excludeFromUpdate, _identityColumn)}" +
                $"{(_outputIdentity == ColumnDirectionType.InputOutput ? $", @{_identityColumn} = [{_identityColumn}] " : string.Empty)} " +
                $"{BulkOperationsHelper.BuildMatchTargetOnList(_matchTargetOn, _collationColumnDic, _customColumnMappings)} " +
                $"IF (@@ROWCOUNT = 0) BEGIN " +
                $"{BulkOperationsHelper.BuildInsertIntoSet(_columns, _identityColumn, fullQualifiedTableName)} " +
                $"VALUES{BulkOperationsHelper.BuildValueSet(_columns, _identityColumn)}" +
                $"{(_outputIdentity == ColumnDirectionType.InputOutput ? $" SET @{_identityColumn} = SCOPE_IDENTITY()" : string.Empty)} END";
        }
    }
}
