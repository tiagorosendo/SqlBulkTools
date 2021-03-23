using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using SqlBulkTools.Core;
using SqlBulkTools.Enumeration;

[assembly: InternalsVisibleTo("SqlBulkTools.UnitTests")]
[assembly: InternalsVisibleTo("SqlBulkTools.IntegrationTests")]

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    public static class BulkOperationsHelper
    {
        internal static Table GetTableAndSchema(string tableName)
        {
            var sb = new StringBuilder(tableName.Trim());

            sb = sb.Replace("[", string.Empty);
            sb = sb.Replace("]", string.Empty);

            var periodCount = sb.ToString().ToCharArray().Count(x => x == '.');

            if (periodCount == 0)
                return new Table {Name = tableName, Schema = Constants.DefaultSchemaName};

            if (periodCount > 1)
                throw new SqlBulkToolsException("Table name can't contain more than one period '.' character.");

            var tableMatch = Regex.Match(sb.ToString(), @"(?<=\.).*");

            // Check if schema is included in table name.
            tableName = tableMatch.Success ? tableMatch.Value : Constants.DefaultSchemaName;

            var schemaMatch = Regex.Match(sb.ToString(), @"^([^.]*)");
            var schema = schemaMatch.Success ? schemaMatch.Value : sb.ToString();

            var table = new Table();
            table.Name = tableName;
            table.Schema = schema;

            return table;
        }

        internal static string GetActualColumn(Dictionary<string, string> customColumnMappings, string propertyName)
        {
            string actualPropertyName;

            if (customColumnMappings.TryGetValue(propertyName, out actualPropertyName))
                return actualPropertyName;

            return propertyName;
        }

        internal static string BuildCreateTempTable(HashSet<string> columns, DataTable schema,
            ColumnDirectionType outputIdentity)
        {
            var actualColumns = new Dictionary<string, string>();
            var actualColumnsMaxCharLength = new Dictionary<string, string>();
            var actualColumnsNumericPrecision = new Dictionary<string, PrecisionType>();
            var actualColumnsDateTimePrecision = new Dictionary<string, string>();

            foreach (DataRow row in schema.Rows)
            {
                var columnType = row["DATA_TYPE"].ToString();
                var columnName = row["COLUMN_NAME"].ToString();

                actualColumns.Add(row["COLUMN_NAME"].ToString(), row["DATA_TYPE"].ToString());

                if (columnType == "varchar" || columnType == "nvarchar" ||
                    columnType == "char" || columnType == "binary" ||
                    columnType == "varbinary" || columnType == "nchar")
                    actualColumnsMaxCharLength.Add(row["COLUMN_NAME"].ToString(),
                        row["CHARACTER_MAXIMUM_LENGTH"].ToString());

                if (columnType == "datetime2" || columnType == "time")
                    actualColumnsDateTimePrecision.Add(row["COLUMN_NAME"].ToString(),
                        row["DATETIME_PRECISION"].ToString());

                if (columnType == "numeric" || columnType == "decimal")
                {
                    var p = new PrecisionType
                    {
                        NumericPrecision = row["NUMERIC_PRECISION"].ToString(),
                        NumericScale = row["NUMERIC_SCALE"].ToString()
                    };
                    actualColumnsNumericPrecision.Add(columnName, p);
                }
            }

            var command = new StringBuilder();

            command.Append($"CREATE TABLE {Constants.TempTableName}(");

            var paramList = new List<string>();

            foreach (var column in columns.ToList().OrderBy(x => x))
            {
                if (column == Constants.InternalId)
                    continue;
                string columnType;
                if (actualColumns.TryGetValue(column, out columnType))
                {
                    columnType = GetVariableCharType(column, columnType, actualColumnsMaxCharLength);
                    columnType = GetDecimalPrecisionAndScaleType(column, columnType, actualColumnsNumericPrecision);
                    columnType = GetDateTimePrecisionType(column, columnType, actualColumnsDateTimePrecision);
                }

                paramList.Add($"[{column}] {columnType}");
            }

            var paramListConcatenated = string.Join(", ", paramList);

            command.Append(paramListConcatenated);

            if (outputIdentity == ColumnDirectionType.InputOutput) command.Append($", [{Constants.InternalId}] int");
            command.Append(");");

            return command.ToString();
        }

        internal static Dictionary<string, bool> GetNullableColumnDic(DataTable schema)
        {
            var nullableDic = new Dictionary<string, bool>();

            foreach (DataRow row in schema.Rows)
            {
                var isColumnNullable = row["IS_NULLABLE"].ToString()
                    .Equals("YES", StringComparison.OrdinalIgnoreCase);

                nullableDic.Add(row["COLUMN_NAME"].ToString(), isColumnNullable);
            }

            return nullableDic;
        }

        private static string GetVariableCharType(string column, string columnType,
            Dictionary<string, string> actualColumnsMaxCharLength)
        {
            if (columnType == "varchar" || columnType == "nvarchar" ||
                columnType == "char" || columnType == "binary" ||
                columnType == "varbinary" || columnType == "nchar")
            {
                string maxCharLength;
                if (actualColumnsMaxCharLength.TryGetValue(column, out maxCharLength))
                {
                    if (maxCharLength == "-1")
                        maxCharLength = "max";

                    columnType = columnType + "(" + maxCharLength + ")";
                }
            }

            return columnType;
        }

        private static string GetDecimalPrecisionAndScaleType(string column, string columnType,
            Dictionary<string, PrecisionType> actualColumnsPrecision)
        {
            if (columnType == "decimal" || columnType == "numeric")
            {
                PrecisionType p;

                if (actualColumnsPrecision.TryGetValue(column, out p))
                    columnType = columnType + "(" + p.NumericPrecision + ", " + p.NumericScale + ")";
            }

            return columnType;
        }

        private static string GetDateTimePrecisionType(string column, string columnType,
            Dictionary<string, string> actualColumnsDateTimePrecision)
        {
            if (columnType == "datetime2" || columnType == "time")
            {
                string dateTimePrecision;
                if (actualColumnsDateTimePrecision.TryGetValue(column, out dateTimePrecision))
                    columnType = columnType + "(" + dateTimePrecision + ")";
            }

            return columnType;
        }

        internal static string BuildJoinConditionsForInsertOrUpdate(string[] updateOn, string sourceAlias,
            string targetAlias, Dictionary<string, string> collationDic, Dictionary<string, bool> nullableColumnDic)
        {
            var command = new StringBuilder();

            command.Append(
                $"ON ([{targetAlias}].[{updateOn[0]}] = [{sourceAlias}].[{updateOn[0]}]{GetCollation(collationDic, updateOn[0])}{BuildNullCondition(updateOn[0], sourceAlias, targetAlias, nullableColumnDic)}) ");

            if (updateOn.Length > 1)
                for (var i = 1; i < updateOn.Length; i++)
                    command.Append(
                        $"AND ([{targetAlias}].[{updateOn[i]}] = [{sourceAlias}].[{updateOn[i]}]{GetCollation(collationDic, updateOn[i])}{BuildNullCondition(updateOn[i], sourceAlias, targetAlias, nullableColumnDic)}) ");

            return command.ToString();
        }

        internal static string BuildNullCondition(string updateOn, string sourceAlias, string targetAlias,
            Dictionary<string, bool> nullableColumnDic)
        {
            bool isColumnNullable;

            if (nullableColumnDic.TryGetValue(updateOn, out isColumnNullable) && isColumnNullable)
                return $" OR ([{targetAlias}].[{updateOn}] IS NULL AND [{sourceAlias}].[{updateOn}] IS NULL)";

            return string.Empty;
        }

        internal static string GetCollation(Dictionary<string, string> collationDic, string column)
        {
            if (collationDic == null)
                return string.Empty;

            string collateColumn = null;
            if (collationDic.TryGetValue(column, out collateColumn)) return $" COLLATE {collateColumn}";

            return string.Empty;
        }

        internal static string BuildMatchTargetOnList(HashSet<string> matchTargetOnColumns,
            Dictionary<string, string> collationDic, Dictionary<string, string> customColumnMappings)
        {
            var sb = new StringBuilder();

            var whereClauseColumn = GetActualColumn(customColumnMappings, matchTargetOnColumns.ElementAt(0));

            sb.Append(
                $"WHERE [{whereClauseColumn}] = @{whereClauseColumn}{GetCollation(collationDic, whereClauseColumn)}");

            if (matchTargetOnColumns.Count() > 1)
                foreach (var column in matchTargetOnColumns)
                {
                    if (column.Equals(matchTargetOnColumns.ElementAt(0)))
                        continue;

                    var andClauseColumn = GetActualColumn(customColumnMappings, column);
                    sb.Append(
                        $" AND [{andClauseColumn}] = @{andClauseColumn}{GetCollation(collationDic, andClauseColumn)}");
                }

            return sb.ToString();
        }

        internal static string BuildPredicateQuery(string[] updateOn, IEnumerable<PredicateCondition> conditions,
            string targetAlias, Dictionary<string, string> collationDic)
        {
            if (conditions == null)
                return null;

            if (updateOn == null || updateOn.Length == 0)
                throw new SqlBulkToolsException("MatchTargetOn is required for AndQuery.");

            var command = new StringBuilder();

            foreach (var condition in conditions)
            {
                var targetColumn = condition.CustomColumnMapping ?? condition.LeftName;

                command.Append(
                    $"AND [{targetAlias}].[{targetColumn}] {GetOperator(condition)} {(condition.Value != "NULL" ? "@" + condition.LeftName + Constants.UniqueParamIdentifier + condition.SortOrder + GetCollation(collationDic, condition.LeftName) : "NULL")} ");
            }

            return command.ToString();
        }

        // Used for UpdateQuery and DeleteQuery where, and, or conditions. 
        internal static string BuildPredicateQuery(IEnumerable<PredicateCondition> conditions,
            Dictionary<string, string> collationDic, Dictionary<string, string> customColumnMappings)
        {
            if (conditions == null)
                return null;


            conditions = conditions.OrderBy(x => x.SortOrder);

            var command = new StringBuilder();

            foreach (var condition in conditions)
            {
                var targetColumn = condition.CustomColumnMapping ?? condition.LeftName;

                if (customColumnMappings != null)
                    targetColumn = GetActualColumn(customColumnMappings, targetColumn);

                switch (condition.PredicateType)
                {
                    case PredicateType.Where:
                    {
                        command.Append(" WHERE ");
                        break;
                    }

                    case PredicateType.And:
                    {
                        command.Append(" AND ");
                        break;
                    }

                    case PredicateType.Or:
                    {
                        command.Append(" OR ");
                        break;
                    }

                    default:
                    {
                        throw new KeyNotFoundException("Predicate not found");
                    }
                }

                command.Append(
                    $"[{targetColumn}] {GetOperator(condition)} {(condition.Value != "NULL" ? "@" + condition.LeftName + Constants.UniqueParamIdentifier + condition.SortOrder + GetCollation(collationDic, condition.LeftName) : "NULL")}");
            }

            return command.ToString();
        }

        internal static string GetOperator(PredicateCondition condition)
        {
            switch (condition.Expression)
            {
                case ExpressionType.NotEqual:
                {
                    if (condition.ValueType == null)
                    {
                        condition.Value = condition.Value?.ToUpper();
                        return "IS NOT";
                    }

                    return "!=";
                }
                case ExpressionType.Equal:
                {
                    if (condition.ValueType == null)
                    {
                        condition.Value = condition.Value?.ToUpper();
                        return "IS";
                    }

                    return "=";
                }
                case ExpressionType.LessThan:
                {
                    return "<";
                }
                case ExpressionType.LessThanOrEqual:
                {
                    return "<=";
                }
                case ExpressionType.GreaterThan:
                {
                    return ">";
                }
                case ExpressionType.GreaterThanOrEqual:
                {
                    return ">=";
                }
            }

            throw new SqlBulkToolsException("ExpressionType not found when trying to map logical operator.");
        }

        internal static string BuildUpdateSet(HashSet<string> columns, string sourceAlias, string targetAlias,
            string identityColumn, HashSet<string> excludeFromUpdate = null)
        {
            var command = new StringBuilder();
            var paramsSeparated = new List<string>();

            if (excludeFromUpdate == null)
                excludeFromUpdate = new HashSet<string>();

            command.Append("SET ");

            foreach (var column in columns.ToList().OrderBy(x => x))
                if ((column != identityColumn || identityColumn == null) && !excludeFromUpdate.Contains(column))
                    if (column != Constants.InternalId)
                        paramsSeparated.Add($"[{targetAlias}].[{column}] = [{sourceAlias}].[{column}]");

            command.Append(string.Join(", ", paramsSeparated) + " ");

            return command.ToString();
        }

        /// <summary>
        ///     Specificially for UpdateQuery and DeleteQuery
        /// </summary>
        /// <param name="columns"></param>
        /// <param name="excludeFromUpdate"></param>
        /// <param name="identityColumn"></param>
        /// <returns></returns>
        public static string BuildUpdateSet(HashSet<string> columns, HashSet<string> excludeFromUpdate,
            string identityColumn)
        {
            var command = new StringBuilder();
            var paramsSeparated = new List<string>();

            // To prevent null reference exception
            if (excludeFromUpdate == null) excludeFromUpdate = new HashSet<string>();

            command.Append("SET ");

            foreach (var column in columns.ToList().OrderBy(x => x))
                if (column != identityColumn && !excludeFromUpdate.Contains(column))
                    paramsSeparated.Add($"[{column}] = @{column}");

            command.Append(string.Join(", ", paramsSeparated));

            return command.ToString();
        }

        internal static string BuildInsertSet(HashSet<string> columns, string sourceAlias, string identityColumn)
        {
            var command = new StringBuilder();
            var insertColumns = new List<string>();
            var values = new List<string>();

            command.Append("INSERT (");

            foreach (var column in columns.ToList().OrderBy(x => x))
                if (column != Constants.InternalId && column != identityColumn)
                {
                    insertColumns.Add($"[{column}]");
                    values.Add($"[{sourceAlias}].[{column}]");
                }

            command.Append($"{string.Join(", ", insertColumns)}) values ({string.Join(", ", values)})");

            return command.ToString();
        }

        internal static string BuildInsertIntoSet(HashSet<string> columns, string identityColumn,
            string fullQualifiedTableName)
        {
            var command = new StringBuilder();
            var insertColumns = new List<string>();

            command.Append($"INSERT INTO {fullQualifiedTableName} (");

            foreach (var column in columns.OrderBy(x => x))
                if (column != Constants.InternalId && column != identityColumn)
                    insertColumns.Add("[" + column + "]");

            command.Append($"{string.Join(", ", insertColumns)}) ");

            return command.ToString();
        }

        internal static string BuildValueSet(HashSet<string> columns, string identityColumn)
        {
            var command = new StringBuilder();
            var valueList = new List<string>();

            command.Append("(");
            foreach (var column in columns.OrderBy(x => x))
                if (column != identityColumn)
                    valueList.Add($"@{column}");
            command.Append(string.Join(", ", valueList));
            command.Append(")");

            return command.ToString();
        }

        internal static string BuildSelectSet(HashSet<string> columns, string sourceAlias, string identityColumn)
        {
            var command = new StringBuilder();
            var selectColumns = new List<string>();

            command.Append("SELECT ");

            foreach (var column in columns.ToList().OrderBy(x => x))
                if (identityColumn != null && column != identityColumn || identityColumn == null)
                    if (column != Constants.InternalId)
                        selectColumns.Add($"[{sourceAlias}].[{column}]");

            command.Append(string.Join(", ", selectColumns));

            return command.ToString();
        }

        internal static string GetPropertyName(Expression method)
        {
            if (!(method is LambdaExpression lambda))
                throw new ArgumentNullException(nameof(method));

            MemberExpression memberExpr = null;

            switch (lambda.Body.NodeType)
            {
                case ExpressionType.Convert:
                    memberExpr =
                        ((UnaryExpression) lambda.Body).Operand as MemberExpression;

                    if (memberExpr?.Expression.Type.GetCustomAttribute(typeof(ComplexTypeAttribute)) != null
                        && memberExpr.Expression is MemberExpression expression)
                        return $"{expression.Member.Name}_{memberExpr.Member.Name}";
                    break;
                case ExpressionType.MemberAccess:
                    memberExpr = lambda.Body as MemberExpression;
                    break;
            }

            if (memberExpr == null)
                throw new ArgumentException("method");

            return memberExpr.Member.Name;
        }

        internal static DataTable CreateDataTable<T>(List<PropInfo> propertyInfoList, HashSet<string> columns,
            Dictionary<string, string> columnMappings, Dictionary<string, int> ordinalDic,
            List<string> matchOnColumns = null, ColumnDirectionType? outputIdentity = null)
        {
            if (columns == null)
                return null;

            var outputIdentityCol = outputIdentity.HasValue &&
                                    outputIdentity.Value == ColumnDirectionType.InputOutput;

            var dataTable = new DataTable(typeof(T).Name);

            if (matchOnColumns != null) columns = CheckForAdditionalColumns(columns, matchOnColumns);

            if (outputIdentityCol) columns.Add(Constants.InternalId);

            foreach (var property in propertyInfoList)
            {
                //TODO: Put some cache here.
                
                var complexTypeAttr = property.PropertyType.GetCustomAttribute(typeof(ComplexTypeAttribute));

                if (complexTypeAttr != null)
                {
                    var complexPropertyInfoList = property.PropertyType.ToPropInfoList();

                    foreach (var complexProperty in complexPropertyInfoList)
                        AddPropertyToDataTable(complexProperty, columnMappings, dataTable, ordinalDic, columns, true,
                            property.Name);
                }
                else
                {
                    AddPropertyToDataTable(property, columnMappings, dataTable, ordinalDic, columns, false, null);
                }

                //var type = Nullable.GetUnderlyingType(property.PropertyType) ??
                //                    property.PropertyType;

                //if (columnMappings != null && columnMappings.ContainsKey(property.Name))
                //{
                //    dataTable.Columns.Add(columnMappings[property.Name], type);
                //    var ordinal = dataTable.Columns[columnMappings[property.Name]].Ordinal;

                //    ordinalDic.Add(property.Name, ordinal);
                //}

                //else if (columns.Contains(property.Name))
                //{
                //    dataTable.Columns.Add(property.Name, type);
                //    var ordinal = dataTable.Columns[property.Name].Ordinal;

                //    ordinalDic.Add(property.Name, ordinal);
                //}          
            }

            if (!outputIdentityCol)
                return dataTable;
            
            dataTable.Columns.Add(Constants.InternalId, typeof(int));
            var ordinal = dataTable.Columns[Constants.InternalId].Ordinal;

            ordinalDic.Add(Constants.InternalId, ordinal);

            return dataTable;
        }

        internal static void AddPropertyToDataTable(PropInfo property, Dictionary<string, string> columnMappings,
            DataTable dataTable, Dictionary<string, int> ordinalDic, HashSet<string> columns, bool isComplex,
            string basePropertyName)
        {
            var propertyName = isComplex ? $"{basePropertyName}_{property.Name}" : property.Name;

            var type = Nullable.GetUnderlyingType(property.PropertyType) ??
                       property.PropertyType;

            if (columnMappings != null && columnMappings.ContainsKey(propertyName))
            {
                dataTable.Columns.Add(columnMappings[propertyName], type);
                var ordinal = dataTable.Columns[columnMappings[propertyName]].Ordinal;

                ordinalDic.Add(propertyName, ordinal);
            }

            else if (columns.Contains(propertyName))
            {
                dataTable.Columns.Add(propertyName, type);
                var ordinal = dataTable.Columns[propertyName].Ordinal;

                ordinalDic.Add(propertyName, ordinal);
            }
        }

        public static DataTable ConvertListToDataTable<T>(List<PropInfo> propertyInfoList, DataTable dataTable,
            IEnumerable<T> list, HashSet<string> columns, Dictionary<string, int> ordinalDic,
            Dictionary<int, T> outputIdentityDic = null)
        {
            var propertyCustomAttributeDictionary
                = propertyInfoList.ToDictionary(pi => pi,
                    pi => pi.PropertyType.GetCustomAttribute(typeof(ComplexTypeAttribute)));

            var internalIdCounter = 0;

            foreach (var item in list)
            {
                var values = new object[columns.Count];
                foreach (var column in columns.ToList())
                {
                    foreach (var property in propertyInfoList)
                    {
                        var res = propertyCustomAttributeDictionary.TryGetValue(property, out var attr);
                        if (res && attr != null)
                        {
                            var complexPropertyList = property.PropertyType.ToPropInfoList();

                            foreach (var complexProperty in complexPropertyList)
                                AddToDataTable(complexProperty, column, item, ordinalDic, values, property.Name, true);
                        }
                        else
                        {
                            AddToDataTable(property, column, item, ordinalDic, values, null, false);
                        }
                    }

                    if (column != Constants.InternalId) 
                        continue;
                    
                    if (ordinalDic.TryGetValue(Constants.InternalId, out var ordinal) == false) 
                        continue;
                        
                    values[ordinal] = internalIdCounter;
                    outputIdentityDic?.Add(internalIdCounter, item);
                }

                internalIdCounter++;
                dataTable.Rows.Add(values);
            }

            return dataTable;
        }

        internal static void AddToDataTable<T>(PropInfo property, string column, T item,
            Dictionary<string, int> ordinalDic,
            object[] values, string basePropertyName, bool isComplex)
        {
            var propertyName = isComplex ? $"{basePropertyName}_{property.Name}" : property.Name;
            if (propertyName == column && item != null &&
                CheckForValidDataType(property.PropertyType, true))
            {
                if (ordinalDic.TryGetValue(propertyName, out var ordinal) == false)
                    return;
                
                if (isComplex)
                {
                    var complexType = item.GetType().GetProperty(basePropertyName);
                    var value = complexType.GetValue(item, null);
                    values[ordinal] = property.GetValue(value);
                }
                else
                {
                    values[ordinal] = property.GetValue(item);
                }
            }
        }

        // Loops through object properties, checks if column has been added, adds as sql parameter. 
        public static void AddSqlParamsForQuery<T>(List<PropInfo> propertyInfoList,
            List<SqlParameter> sqlParameters, HashSet<string> columns, T item,
            string identityColumn = null, ColumnDirectionType direction = ColumnDirectionType.Input,
            Dictionary<string, string> customColumns = null)
        {
            foreach (var column in columns.ToList().OrderBy(x => x))
            foreach (var property in propertyInfoList)
                if (property.PropertyType.GetCustomAttribute(typeof(ComplexTypeAttribute)) != null)
                {
                    var complexPropertyList = property.PropertyType.ToPropInfoList();
                        foreach (var complexProperty in complexPropertyList)
                    {
                        var propertyName = $"{property.Name}_{complexProperty.Name}";

                        if (propertyName == column && item != null &&
                            CheckForValidDataType(complexProperty.PropertyType, true))
                        {
                            var param = GetSqlParam<T>(complexProperty, customColumns, column);

                            var complexType = item.GetType().GetProperty(property.Name);
                            var value = complexType.GetValue(item, null);

                            var propertyInfo = complexType.PropertyType.GetProperty(complexProperty.Name);
                            var propValue = propertyInfo.GetValue(value, null);

                            param.Value = propValue ?? DBNull.Value;

                            sqlParameters.Add(param);
                        }
                    }
                }

                else if (property.Name == column && item != null && CheckForValidDataType(property.PropertyType, true))
                {
                    var param = GetSqlParam<T>(property, customColumns, column);

                    var propValue = property.GetValue(item);

                    param.Value = propValue ?? DBNull.Value;

                    if (column == identityColumn && direction == ColumnDirectionType.InputOutput)
                        param.Direction = ParameterDirection.InputOutput;

                    sqlParameters.Add(param);
                }
        }

        private static SqlParameter GetSqlParam<T>(PropInfo property, Dictionary<string, string> customColumns,
            string column)
        {
            var sqlType = BulkOperationsUtility.GetSqlTypeFromDotNetType(property.PropertyType);

            SqlParameter param;

            if (customColumns != null && customColumns.TryGetValue(column, out var actualColumnName))
                param = new SqlParameter($"@{actualColumnName}", sqlType);
            else
                param = new SqlParameter($"@{column}", sqlType);

            return param;
        }

        /// <summary>
        /// </summary>
        /// <param name="type"></param>
        /// <param name="throwIfInvalid">
        ///     Set this to true if user is manually adding columns. If AddAllColumns is used, then this can be omitted.
        /// </param>
        /// <returns></returns>
        private static bool CheckForValidDataType(Type type, bool throwIfInvalid = false)
        {
            if (type.IsValueType ||
                type == typeof(string) ||
                type == typeof(byte[]) ||
                type == typeof(char[]) ||
//                type == typeof(SqlGeometry) || TODO: Review.
//                type == typeof(SqlGeography) ||
                type == typeof(SqlXml)
            )
                return true;

            if (throwIfInvalid)
                throw new SqlBulkToolsException(
                    $"Only value, string, char[], byte[], SqlGeometry, SqlGeography and SqlXml types can be used " +
                    $"with SqlBulkTools. Refer to https://msdn.microsoft.com/en-us/library/cc716729(v=vs.110).aspx for " +
                    $"more details.");

            return false;
        }

        internal static string GetFullQualifyingTableName(string databaseName, string schemaName, string tableName)
        {
            var sb = new StringBuilder();
            sb.Append("[");
            sb.Append(databaseName);
            sb.Append("].[");
            sb.Append(schemaName);
            sb.Append("].[");
            sb.Append(tableName);
            sb.Append("]");

            return sb.ToString();
        }


        /// <summary>
        ///     If there are MatchOnColumns that don't exist in columns, add to columns.
        /// </summary>
        /// <param name="columns"></param>
        /// <param name="matchOnColumns"></param>
        /// <returns></returns>
        private static HashSet<string> CheckForAdditionalColumns(HashSet<string> columns, List<string> matchOnColumns)
        {
            foreach (var col in matchOnColumns)
                if (!columns.Contains(col))
                    columns.Add(col);

            return columns;
        }

        internal static void DoColumnMappings(Dictionary<string, string> columnMappings, HashSet<string> columns,
            List<string> updateOnList)
        {
            if (columnMappings.Count <= 0)
                return;

            foreach (var column in columnMappings)
            {
                if (columns.Contains(column.Key))
                {
                    columns.Remove(column.Key);
                    columns.Add(column.Value);
                }

                for (var i = 0; i < updateOnList.ToArray().Length; i++)
                    if (updateOnList[i] == column.Key)
                        updateOnList[i] = column.Value;
            }
        }

        internal static void DoColumnMappings(Dictionary<string, string> columnMappings, HashSet<string> columns)
        {
            if (columnMappings.Count <= 0) 
                return;
            
            foreach (var column in columnMappings)
                if (columns.Contains(column.Key))
                {
                    columns.Remove(column.Key);
                    columns.Add(column.Value);
                }
        }

        internal static void DoColumnMappings(Dictionary<string, string> columnMappings,
            IEnumerable<PredicateCondition> predicateConditions)
        {
            foreach (var condition in predicateConditions)
            {
                if (columnMappings.TryGetValue(condition.LeftName, out var columnName))
                    condition.CustomColumnMapping = columnName;
            }
        }

        /// <summary>
        ///     Advanced Settings for SQLBulkCopy class.
        /// </summary>
        /// <param name="bulkcopy"></param>
        /// <param name="options"></param>
        internal static void SetSqlBulkCopySettings(SqlBulkCopy bulkcopy, BulkCopySettings options)
        {
            bulkcopy.EnableStreaming = options.EnableStreaming;
            bulkcopy.BatchSize = options.BatchSize;
            bulkcopy.BulkCopyTimeout = options.BulkCopyTimeout;

            if (options.BulkCopyNotification == null) 
                return;
            
            bulkcopy.NotifyAfter = options.BulkCopyNotification.NotifyAfter;
            bulkcopy.SqlRowsCopied += options.BulkCopyNotification.SqlRowsCopied;
        }

        /// <summary>
        ///     This is used only for the BulkInsert method at this time.
        /// </summary>
        /// <param name="bulkCopy"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        internal static void MapColumns(SqlBulkCopy bulkCopy, HashSet<string> columns,
            Dictionary<string, string> customColumnMappings)
        {
            foreach (var column in columns.ToList().OrderBy(x => x))
            {
                if (customColumnMappings.TryGetValue(column, out var mapping))
                    bulkCopy.ColumnMappings.Add(mapping, mapping);

                else
                    bulkCopy.ColumnMappings.Add(column, column);
            }
        }

        internal static HashSet<string> GetAllValueTypeAndStringColumns(List<PropInfo> propertyInfoList, Type type)
        {
            var columns = new HashSet<string>();

            foreach (var property in propertyInfoList)
            {
                //property.PropertyType.CustomAttributes.First(x => x.AttributeType.)

                var complexTypeAttr = property.PropertyType.GetCustomAttribute(typeof(ComplexTypeAttribute));
                var generatedTypeAttr = (DatabaseGeneratedAttribute)property.PropertyType.GetCustomAttribute(typeof(DatabaseGeneratedAttribute));

                if (complexTypeAttr != null)
                {
                    var complexTypeProperties = property.PropertyType.GetProperties();

                    foreach (var complexProperty in complexTypeProperties)
                        if (CheckForValidDataType(complexProperty.PropertyType))
                            columns.Add($"{property.Name}_{complexProperty.Name}");
                }

                else if (generatedTypeAttr != null && generatedTypeAttr.DatabaseGeneratedOption == DatabaseGeneratedOption.Computed)
                {
                    // do not add
                }

                else if (CheckForValidDataType(property.PropertyType))
                {
                    columns.Add(property.Name);
                }
            }

            return columns;
        }

        internal static string GetOutputIdentityCmd(string identityColumn, ColumnDirectionType outputIdentity,
            string tmpTableName, OperationType operation)
        {
            var sb = new StringBuilder();
            
            if (identityColumn == null || outputIdentity != ColumnDirectionType.InputOutput) 
                return null;
            
            switch (operation)
            {
                case OperationType.Insert:
                    sb.Append($"OUTPUT inserted.{identityColumn} INTO {tmpTableName}({identityColumn}); ");
                    break;
                case OperationType.InsertOrUpdate:
                case OperationType.Update:
                    sb.Append(
                        $"OUTPUT Source.{Constants.InternalId}, inserted.{identityColumn} INTO {tmpTableName}({Constants.InternalId}, {identityColumn}); ");
                    break;
                case OperationType.Delete:
                    sb.Append(
                        $"OUTPUT Source.{Constants.InternalId}, deleted.{identityColumn} INTO {tmpTableName}({Constants.InternalId}, {identityColumn}); ");
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(operation), operation, null);
            }

            return sb.ToString();
        }

        internal static string GetOutputCreateTableCmd(ColumnDirectionType outputIdentity, string tmpTablename,
            OperationType operation, string identityColumn)
        {
            switch (operation)
            {
                case OperationType.Insert:
                    return outputIdentity == ColumnDirectionType.InputOutput
                        ? $"CREATE TABLE {tmpTablename}([{identityColumn}] int); "
                        : string.Empty;
                case OperationType.InsertOrUpdate:
                case OperationType.Update:
                case OperationType.Delete:
                    return outputIdentity == ColumnDirectionType.InputOutput
                        ? $"CREATE TABLE {tmpTablename}([{Constants.InternalId}] int, [{identityColumn}] int); "
                        : string.Empty;
                default:
                    return string.Empty;

            }
        }

        internal static string GetDropTmpTableCmd()
        {
            return $"DROP TABLE {Constants.TempOutputTableName};";
        }

        internal static string GetIndexManagementCmd(string action, string tableName,
            string schema, IDbConnection conn)
        {
            var cmd =
                $"DECLARE @sql AS VARCHAR(MAX)=''; SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' {action};' FROM sys.indexes " +
                $"JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id WHERE sys.indexes.type_desc = 'NONCLUSTERED' AND sys.objects.type_desc = 'USER_TABLE' " +
                $"AND sys.objects.name = '{GetFullQualifyingTableName(conn.Database, schema, tableName)}'; EXEC(@sql);";

            return cmd;
        }

        /// <summary>
        ///     Gets schema information for a table. Used to get SQL type of property.
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="schema"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        internal static DataTable GetDatabaseSchema(BulkOperations bulk, SqlConnection conn, string schema, string tableName)
        {
            return bulk.Prepare(conn, schema, tableName);
        }

        internal static void InsertToTmpTable(SqlConnection conn, DataTable dt, BulkCopySettings bulkCopySettings, SqlTransaction transaction)
        {
            using (var bulkcopy = new SqlBulkCopy(conn, bulkCopySettings.SqlBulkCopyOptions, transaction))
            {
                bulkcopy.DestinationTableName = Constants.TempTableName;

                SetSqlBulkCopySettings(bulkcopy, bulkCopySettings);

                foreach (var column in dt.Columns) bulkcopy.ColumnMappings.Add(column.ToString(), column.ToString());

                bulkcopy.WriteToServer(dt);
            }
        }

        internal static async Task InsertToTmpTableAsync(SqlConnection conn, DataTable dt, BulkCopySettings bulkCopySettings, SqlTransaction transaction, CancellationToken cancellationToken)
        {
            using (var bulkcopy = new SqlBulkCopy(conn, bulkCopySettings.SqlBulkCopyOptions, transaction))
            {
                bulkcopy.DestinationTableName = Constants.TempTableName;

                SetSqlBulkCopySettings(bulkcopy, bulkCopySettings);

                foreach (var column in dt.Columns) bulkcopy.ColumnMappings.Add(column.ToString(), column.ToString());

                await bulkcopy.WriteToServerAsync(dt, cancellationToken).ConfigureAwait(false);
            }
        }

        internal static void LoadFromTmpOutputTable<T>(SqlCommand command, List<PropInfo> propertyInfoList, string identityColumn,
            Dictionary<int, T> outputIdentityDic, OperationType operationType, IEnumerable<T> list)
        {
            var identityProperty = propertyInfoList.Single(p => p.Name == identityColumn);
            if (!identityProperty.CanWrite)
                throw new SqlBulkToolsException(GetPrivateSetterExceptionMessage(identityColumn));

            switch (operationType)
            {
                case OperationType.InsertOrUpdate:
                case OperationType.Update:
                case OperationType.Delete:
                    command.CommandText =
                        $"SELECT {Constants.InternalId}, {identityColumn} FROM " +
                        $"{Constants.TempOutputTableName} WHERE {Constants.InternalId} IS NOT NULL;";

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            if (outputIdentityDic.TryGetValue(reader.GetInt32(0), out var item))
                                identityProperty.SetValue(item, reader.GetInt32(1));
                        }
                    }

                    command.CommandText = GetDropTmpTableCmd();
                    command.ExecuteNonQuery();
                    break;
                case OperationType.Insert:
                    command.CommandText =
                        $"SELECT {identityColumn} FROM {Constants.TempOutputTableName} ORDER BY {identityColumn};";

                    using (var reader = command.ExecuteReader())
                    {
                        var items = list.ToList();
                        var counter = 0;

                        while (reader.Read())
                        {
                            identityProperty.SetValue(items[counter], reader.GetInt32(0));
                            counter++;
                        }
                    }

                    command.CommandText = GetDropTmpTableCmd();
                    command.ExecuteNonQuery();
                    break;
            }
        }

        internal static async Task LoadFromTmpOutputTableAsync<T>(SqlCommand command, List<PropInfo> propertyInfoList, string identityColumn,
            Dictionary<int, T> outputIdentityDic, OperationType operationType, IEnumerable<T> list, CancellationToken cancellationToken)
        {
            var identityProperty = propertyInfoList.Single(p => p.Name == identityColumn);
            if (!identityProperty.CanWrite)
                throw new SqlBulkToolsException(GetPrivateSetterExceptionMessage(identityColumn));

            switch (operationType)
            {
                case OperationType.InsertOrUpdate:
                case OperationType.Update:
                case OperationType.Delete:
                    command.CommandText =
                        $"SELECT {Constants.InternalId}, {identityColumn} FROM " +
                        $"{Constants.TempOutputTableName} WHERE {Constants.InternalId} IS NOT NULL;";

                    using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                    {
                        while (reader.Read())
                        {
                            if (outputIdentityDic.TryGetValue(reader.GetInt32(0), out var item))
                                identityProperty.SetValue(item, reader.GetInt32(1));
                        }
                    }

                    command.CommandText = GetDropTmpTableCmd();
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    break;
                case OperationType.Insert:
                    command.CommandText =
                        $"SELECT {identityColumn} FROM {Constants.TempOutputTableName} ORDER BY {identityColumn};";

                    using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var items = list.ToList();
                        var counter = 0;

                        while (reader.Read())
                        {
                            identityProperty.SetValue(items[counter], reader.GetInt32(0));
                            counter++;
                        }
                    }

                    command.CommandText = GetDropTmpTableCmd();
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    break;
            }
        }

        private static string GetPrivateSetterExceptionMessage(string columnName)
        {
            return $"No setter method available on property '{columnName}'. Could not write output back to property.";
        }

        internal static string GetInsertIntoStagingTableCmd(SqlConnection conn, string schema,
            string tableName,
            HashSet<string> columns, string identityColumn, ColumnDirectionType outputIdentity)
        {
            var fullTableName = GetFullQualifyingTableName(conn.Database, schema,
                tableName);

            var comm =
                GetOutputCreateTableCmd(outputIdentity, Constants.TempOutputTableName,
                    OperationType.Insert, identityColumn) +
                BuildInsertIntoSet(columns, identityColumn, fullTableName)
                + "OUTPUT INSERTED.[" + identityColumn + "] INTO "
                + Constants.TempOutputTableName + "([" + identityColumn + "]) "
                + BuildSelectSet(columns, Constants.SourceAlias, identityColumn)
                + " FROM " + Constants.TempTableName + " AS Source ORDER BY "+ Constants.InternalId + "; " +
                "DROP TABLE " + Constants.TempTableName + ";";

            return comm;
        }

        /// <summary>
        /// </summary>
        /// <param name="predicate"></param>
        /// <param name="predicateType"></param>
        /// <param name="predicateList"></param>
        /// <param name="sqlParamsList"></param>
        /// <param name="sortOrder"></param>
        /// <param name="appendParam"></param>
        internal static void AddPredicate<T>(Expression<Func<T, bool>> predicate, PredicateType predicateType,
            List<PredicateCondition> predicateList,
            List<SqlParameter> sqlParamsList, int sortOrder, string appendParam)
        {
            string value;
            PredicateCondition condition;

            var binaryBody = predicate.Body as BinaryExpression;

            if (binaryBody == null)
                throw new SqlBulkToolsException(
                    $"Expression not supported for {GetPredicateMethodName(predicateType)}");

            var leftName = ((MemberExpression) binaryBody.Left).Member.Name;

            if (((MemberExpression) binaryBody.Left).Expression.Type.GetCustomAttribute(typeof(ComplexTypeAttribute)) !=
                null
                && ((MemberExpression) binaryBody.Left).Expression is MemberExpression)
                leftName =
                    $"{((MemberExpression) ((MemberExpression) binaryBody.Left).Expression).Member.Name}_{leftName}";

            // For expression types Equal and NotEqual, it's possible for user to pass null value. This handles the null use case. 
            // SqlParameter is not added when comparison to null value is used. 
            switch (predicate.Body.NodeType)
            {
                case ExpressionType.NotEqual:
                {
                    //leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                    value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();


                    if (value != null)
                    {
                        condition = new PredicateCondition
                        {
                            Expression = ExpressionType.NotEqual,
                            LeftName = leftName,
                            ValueType = binaryBody.Right.Type,
                            Value = value,
                            PredicateType = predicateType,
                            SortOrder = sortOrder
                        };

                        var sqlType = BulkOperationsUtility.GetSqlTypeFromDotNetType(condition.ValueType);

                        var paramName = appendParam != null ? leftName + appendParam + sortOrder : leftName;
                        var param = new SqlParameter($"@{paramName}", sqlType);
                        param.Value = condition.Value;
                        sqlParamsList.Add(param);
                    }
                    else
                    {
                        condition = new PredicateCondition
                        {
                            Expression = ExpressionType.NotEqual,
                            LeftName = leftName,
                            Value = "NULL",
                            PredicateType = predicateType,
                            SortOrder = sortOrder
                        };
                    }

                    predicateList.Add(condition);


                    break;
                }

                // For expression types Equal and NotEqual, it's possible for user to pass null value. This handles the null use case. 
                // SqlParameter is not added when comparison to null value is used. 
                case ExpressionType.Equal:
                {
                    //leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                    value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();

                    if (value != null)
                    {
                        condition = new PredicateCondition
                        {
                            Expression = ExpressionType.Equal,
                            LeftName = leftName,
                            ValueType = binaryBody.Right.Type,
                            Value = value,
                            PredicateType = predicateType,
                            SortOrder = sortOrder
                        };

                        var sqlType = BulkOperationsUtility.GetSqlTypeFromDotNetType(condition.ValueType);
                        var paramName = appendParam != null ? leftName + appendParam + sortOrder : leftName;
                        var param = new SqlParameter($"@{paramName}", sqlType);
                        param.Value = condition.Value;
                        sqlParamsList.Add(param);
                    }
                    else
                    {
                        condition = new PredicateCondition
                        {
                            Expression = ExpressionType.Equal,
                            LeftName = leftName,
                            Value = "NULL",
                            PredicateType = predicateType,
                            SortOrder = sortOrder
                        };
                    }

                    predicateList.Add(condition);

                    break;
                }
                case ExpressionType.LessThan:
                {
                    //leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                    value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();
                    BuildCondition(leftName, value, binaryBody.Right.Type, ExpressionType.LessThan, predicateList,
                        sqlParamsList,
                        predicateType, sortOrder, appendParam);
                    break;
                }
                case ExpressionType.LessThanOrEqual:
                {
                    //leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                    value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();
                    BuildCondition(leftName, value, binaryBody.Right.Type, ExpressionType.LessThanOrEqual,
                        predicateList,
                        sqlParamsList, predicateType, sortOrder, appendParam);
                    break;
                }
                case ExpressionType.GreaterThan:
                {
                    //leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                    value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();
                    BuildCondition(leftName, value, binaryBody.Right.Type, ExpressionType.GreaterThan, predicateList,
                        sqlParamsList, predicateType, sortOrder, appendParam);
                    break;
                }
                case ExpressionType.GreaterThanOrEqual:
                {
                    //leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                    value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();
                    BuildCondition(leftName, value, binaryBody.Right.Type, ExpressionType.GreaterThanOrEqual,
                        predicateList,
                        sqlParamsList, predicateType, sortOrder, appendParam);
                    break;
                }
                case ExpressionType.AndAlso:
                {
                    throw new SqlBulkToolsException(
                        $"And && expression not supported for {GetPredicateMethodName(predicateType)}. " +
                        $"Try chaining predicates instead e.g. {GetPredicateMethodName(predicateType)}." +
                        $"{GetPredicateMethodName(predicateType)}");
                }
                case ExpressionType.OrElse:
                {
                    throw new SqlBulkToolsException(
                        $"Or || expression not supported for {GetPredicateMethodName(predicateType)}.");
                }

                default:
                {
                    throw new SqlBulkToolsException(
                        $"Expression used in {GetPredicateMethodName(predicateType)} not supported. " +
                        $"Only == != < <= > >= expressions are accepted.");
                }
            }
        }

        internal static string GetExpressionLeftName<T>(Expression<Func<T, bool>> predicate,
            PredicateType predicateType, string columnType)
        {
            var binaryBody = predicate.Body as BinaryExpression;

            if (binaryBody == null)
                throw new SqlBulkToolsException(
                    $"Expression not supported for {GetPredicateMethodName(predicateType)}");

            var leftName = ((MemberExpression) binaryBody.Left).Member.Name;

            if (leftName == null) throw new SqlBulkToolsException($"{columnType} can't be null");

            return leftName;
        }

        /// <summary>
        /// </summary>
        /// <param name="predicateType"></param>
        /// <returns></returns>
        internal static string GetPredicateMethodName(PredicateType predicateType)
        {
            return predicateType == PredicateType.Update
                ? "UpdateWhen(...)"
                : predicateType == PredicateType.Delete
                    ? "DeleteWhen(...)"
                    : predicateType == PredicateType.Where
                        ? "Where(...)"
                        : predicateType == PredicateType.And
                            ? "And(...)"
                            : predicateType == PredicateType.Or
                                ? "Or(...)"
                                : string.Empty;
        }

        /// <summary>
        /// </summary>
        /// <param name="leftName"></param>
        /// <param name="value"></param>
        /// <param name="valueType"></param>
        /// <param name="expressionType"></param>
        /// <param name="predicateList"></param>
        /// <param name="sqlParamsList"></param>
        /// <param name="sortOrder"></param>
        /// <param name="appendParam"></param>
        /// <param name="predicateType"></param>
        private static void BuildCondition(string leftName, string value, Type valueType,
            ExpressionType expressionType,
            List<PredicateCondition> predicateList, List<SqlParameter> sqlParamsList, PredicateType predicateType,
            int sortOrder, string appendParam)
        {
            var condition = new PredicateCondition
            {
                Expression = expressionType,
                LeftName = leftName,
                ValueType = valueType,
                Value = value,
                PredicateType = predicateType,
                SortOrder = sortOrder
            };

            predicateList.Add(condition);


            var sqlType = BulkOperationsUtility.GetSqlTypeFromDotNetType(condition.ValueType);
            var paramName = appendParam != null ? leftName + appendParam + sortOrder : leftName;
            var param = new SqlParameter($"@{paramName}", sqlType);
            param.Value = condition.Value;
            sqlParamsList.Add(param);
        }

        internal struct PrecisionType
        {
            public string NumericPrecision { get; set; }
            public string NumericScale { get; set; }
        }
    }
}