using System.Collections.Generic;
using System.Data.SqlClient;
using SqlBulkTools.QueryOperations;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class Setup
    {
        private readonly BulkOperations _ext; 
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ext"></param>
        public Setup(BulkOperations ext)
        {
            _ext = ext;
        }

        /// <summary>
        /// Represents the collection of objects to be inserted/upserted/updated/deleted (configured in next steps). 
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public BulkForCollection<T> ForCollection<T>(IEnumerable<T> list) where T : class
        {
            return new BulkForCollection<T>(_ext, list);
        }

    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Setup<T> where T : class
    {
        private readonly BulkOperations _ext;

        private readonly List<SqlParameter> _sqlParams;
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ext"></param>
        public Setup(BulkOperations ext)
        {
            _ext = ext;
            _sqlParams = new List<SqlParameter>();
        }

        ///// <summary>
        ///// Use this option for simple updates or deletes where you are only dealing with a single table 
        ///// and conditions are not complex. For anything more advanced, use a stored procedure.  
        ///// </summary>
        ///// <param name="entity"></param>
        ///// <returns></returns>

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DeleteQuery<T> ForDeleteQuery()
        {
            return new DeleteQuery<T>();
        }

        /// <summary>
        /// Represents the collection of objects to be inserted/upserted/updated/deleted (configured in next steps). 
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public BulkForCollection<T> ForCollection(IEnumerable<T> list)
        {
            return new BulkForCollection<T>(_ext, list);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public QueryForObject<T> ForObject(T entity)
        {
            return new QueryForObject<T>(entity, _sqlParams);
        }
       
    }
}
