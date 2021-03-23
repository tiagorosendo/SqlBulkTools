using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BulkAddColumn<T> : AbstractColumnSelection<T>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="schema"></param>
        /// <param name="bulkCopySettings"></param>
        /// <param name="propertyInfoList"></param>
        public BulkAddColumn(BulkOperations bulk, IEnumerable<T> list, string tableName, HashSet<string> columns, Dictionary<string, string> customColumnMappings, string schema, BulkCopySettings bulkCopySettings, List<PropInfo> propertyInfoList) :
            base(bulk, list, tableName, columns, customColumnMappings, schema, bulkCopySettings, propertyInfoList)
        {

        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        /// procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public BulkAddColumn<T> AddColumn(string columnName)
        {
            _columns.Add(columnName);
            return this;
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        /// procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public BulkAddColumn<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            return AddColumn(BulkOperationsHelper.GetPropertyName(columnName));
        }

        public BulkAddColumn<T> AddColumns(params string[] columnNames)
        {
            foreach (var column in columnNames)
                AddColumn(column);
            return this;
        }

        public BulkAddColumn<T> AddColumns(params Expression<Func<T, object>>[] columnNames)
        {
            foreach (var column in columnNames)
                AddColumn(column);
            return this;
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        /// procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <param name="destination">The actual name of column as represented in SQL table. By default SqlBulkTools will attempt to match the model property names to SQL column names (case insensitive). 
        /// If any of your model property names do not match 
        /// the SQL table column(s) as defined in given table, then use this overload to set up a custom mapping. </param>
        /// <returns></returns>
        public BulkAddColumn<T> AddColumn(string columnName, string destination)
        {
            if (destination == null)
                throw new ArgumentNullException(nameof(destination));

            _customColumnMappings.Add(columnName, destination);

            return AddColumn(columnName);
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        /// procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <param name="destination">The actual name of column as represented in SQL table. By default SqlBulkTools will attempt to match the model property names to SQL column names (case insensitive). 
        /// If any of your model property names do not match 
        /// the SQL table column(s) as defined in given table, then use this overload to set up a custom mapping. </param>
        /// <returns></returns>
        public BulkAddColumn<T> AddColumn(Expression<Func<T, object>> columnName, string destination)
        {
            return AddColumn(BulkOperationsHelper.GetPropertyName(columnName), destination);
        }
    }
}
