using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class DataTableColumns<T>
    {
        private HashSet<string> Columns { get; set; }
        private readonly IEnumerable<T> _list;
        private readonly DataTableOperations _ext;
        private readonly Dictionary<string, int> _ordinalDic;
        private readonly List<PropertyInfo> _propertyInfoList;

        /// <summary>
        /// 
        /// </summary>
        public DataTableColumns(IEnumerable<T> list, DataTableOperations ext)
        {
            _list = list;
            _ext = ext;
            Columns = new HashSet<string>();
            _ordinalDic = new Dictionary<string, int>();
            _propertyInfoList = typeof(T).GetProperties().OrderBy(x => x.Name).ToList();
        }

        /// <summary>
        /// Add each column that you want to include in the DataTable manually. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public DataTableSingularColumnSelect<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            Columns.Add(propertyName);
            return new DataTableSingularColumnSelect<T>(_ext, _list, Columns, _ordinalDic, _propertyInfoList);
        }

        /// <summary>
        /// Adds all properties in model that are either value, string, char[] or byte[] type. 
        /// </summary>
        /// <returns></returns>
        public DataTableAllColumnSelect<T> AddAllColumns()
        {
            Columns = BulkOperationsHelper.GetAllValueTypeAndStringColumns(_propertyInfoList);
            return new DataTableAllColumnSelect<T>(_ext, _list, Columns, _ordinalDic, _propertyInfoList);
        }

    }
}
