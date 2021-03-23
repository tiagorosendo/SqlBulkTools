using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace SqlBulkTools
{
    internal static class PropInfoList
    {
        public static List<PropInfo> ToPropInfoList(this Type type) =>
            type.GetProperties().OrderBy(x => x.Name).Select(p => new PropInfo(p)).ToList();

        public static List<PropInfo> ToPropInfoList(this Dictionary<string, Type> dictionary) =>
            dictionary.Select(p => new PropInfo(p.Key, p.Value)).ToList();

        public static List<PropInfo> From<T>(Dictionary<string, Type> propTypes) =>
            typeof(IDictionary<string, object>).IsAssignableFrom(typeof(T))
                ? propTypes == null
                    ? throw new SqlBulkToolsException("Need property types when entity type is IDictionary<string, object>.")
                    : propTypes.ToPropInfoList()
                : typeof(T).ToPropInfoList();
    }

    public class PropInfo
    {
        private readonly PropertyInfo _propertyInfo;

        internal PropInfo(PropertyInfo propertyInfo) => 
            (_propertyInfo, Name, PropertyType) = (propertyInfo, propertyInfo.Name, propertyInfo.PropertyType);

        internal PropInfo(string name, Type type) =>
            (Name, PropertyType) = (name, type);

        public string Name { get; }
        public Type PropertyType { get; }

        public object GetValue(object entity) =>
            _propertyInfo != null
            ? _propertyInfo.GetValue(entity, null)
            : entity is IDictionary<string, object> dict
            ? dict[Name]
            : null;

        public void SetValue(object entity, object value)
        {
            if (_propertyInfo != null)
            {
                _propertyInfo.SetValue(entity, value, null);
            }
            else if (entity is IDictionary<string, object> dict)
            {
                dict[Name] = value;
            }
        }

        public bool CanWrite => _propertyInfo?.CanWrite ?? true;
    }
}
