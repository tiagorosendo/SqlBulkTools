using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DataTableSingularColumnSelect<T> : DataTableAbstractColumnSelect<T>, IDataTableTransaction
    {
        private Dictionary<string, int> _ordinalDic;
        private List<PropInfo> _propertyInfoList;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ext"></param>
        /// <param name="list"></param>
        /// <param name="columns"></param>
        /// <param name="ordinalDic"></param>
        /// <param name="propertyInfoList"></param>
        public DataTableSingularColumnSelect(DataTableOperations ext, IEnumerable<T> list, HashSet<string> columns, Dictionary<string, int> ordinalDic, List<PropInfo> propertyInfoList) : base(ext, list, columns)
        {
            _ordinalDic = ordinalDic;
            _propertyInfoList = propertyInfoList;
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        /// procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public DataTableSingularColumnSelect<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            _columns.Add(propertyName);
            return this;
        }

        /// <summary>
        /// If a column name in your model does not match the designated column name in the actual SQL table, 
        /// you can add a custom column mapping. 
        /// </summary>
        /// <returns></returns>
        public DataTableSingularColumnSelect<T> CustomColumnMapping(Expression<Func<T, object>> source, string destination)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(source);
            CustomColumnMappings.Add(propertyName, destination);
            return this;
        }

        /// <summary>
        /// Returns a data table to be used in a stored procedure as table variable or temp table.
        /// Make any neccessary changes before calling BuildPreparedDataTable
        /// </summary>
        /// <returns></returns>
        public DataTable PrepareDataTable()
        {
            _dt = BulkOperationsHelper.CreateDataTable<T>(_propertyInfoList, _columns, CustomColumnMappings, _ordinalDic);
            _ext.SetBulkExt(this, _columns, CustomColumnMappings, typeof(T));
            return _dt;
        }

        DataTable IDataTableTransaction.BuildDataTable()
        {
            return BulkOperationsHelper.ConvertListToDataTable(_propertyInfoList, _dt, _list, _columns, _ordinalDic);
        }

    }
}
