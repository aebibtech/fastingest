using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

namespace FastIngest.Core.Mapping;

public sealed class ColumnMapping<TRecord>
{
    public PropertyInfo Property { get; }
    public string PropertyName => Property.Name;
    public string ColumnName { get; }
    public int? ColumnIndex { get; }
    public Type PropertyType => Property.PropertyType;
    public Func<TRecord, object?> Getter { get; }
    public Action<TRecord, object?>? Setter { get; }

    public ColumnMapping(PropertyInfo property, string columnName, int? columnIndex = null)
    {
        Property = property ?? throw new ArgumentNullException(nameof(property));
        ColumnName = string.IsNullOrWhiteSpace(columnName) ? property.Name : columnName;
        ColumnIndex = columnIndex;
        Getter = CreateGetter(property);
        Setter = CreateSetter(property);
    }

    private static Func<TRecord, object?> CreateGetter(PropertyInfo property)
    {
        var param = Expression.Parameter(typeof(TRecord), "record");
        var propAccess = Expression.Property(param, property);
        var convert = Expression.Convert(propAccess, typeof(object));
        return Expression.Lambda<Func<TRecord, object?>>(convert, param).Compile();
    }

    private static Action<TRecord, object?>? CreateSetter(PropertyInfo property)
    {
        if (!property.CanWrite || property.SetMethod == null)
            return null;

        var instanceParam = Expression.Parameter(typeof(TRecord), "instance");
        var valueParam = Expression.Parameter(typeof(object), "value");
        var convertedValue = Expression.Convert(valueParam, property.PropertyType);
        var call = Expression.Call(instanceParam, property.SetMethod, convertedValue);
        return Expression.Lambda<Action<TRecord, object?>>(call, instanceParam, valueParam).Compile();
    }
}

public sealed class ColumnMappingBuilder<TRecord>
{
    private readonly Dictionary<string, ColumnMapping<TRecord>> _mappings = new(StringComparer.OrdinalIgnoreCase);

    public ColumnMappingBuilder<TRecord> Map<TProperty>(
        Expression<Func<TRecord, TProperty>> propertyExpression,
        string? columnName = null)
    {
        var property = ExtractProperty(propertyExpression);
        var mapping = new ColumnMapping<TRecord>(property, columnName ?? property.Name);
        _mappings[property.Name] = mapping;
        return this;
    }

    public ColumnMappingBuilder<TRecord> Map<TProperty>(
        Expression<Func<TRecord, TProperty>> propertyExpression,
        int columnIndex,
        string? columnName = null)
    {
        var property = ExtractProperty(propertyExpression);
        var mapping = new ColumnMapping<TRecord>(property, columnName ?? property.Name, columnIndex);
        _mappings[property.Name] = mapping;
        return this;
    }

    public IReadOnlyList<ColumnMapping<TRecord>> Build()
    {
        if (_mappings.Count > 0)
        {
            return _mappings.Values.ToList();
        }

        // Default: reflect all public instance properties
        return CreateDefaultMappings();
    }

    public static IReadOnlyList<ColumnMapping<TRecord>> CreateDefaultMappings()
    {
        var properties = typeof(TRecord).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        return properties.Select(p => new ColumnMapping<TRecord>(p, p.Name)).ToList();
    }

    private static PropertyInfo ExtractProperty<TProperty>(Expression<Func<TRecord, TProperty>> propertyExpression)
    {
        MemberExpression? memberExpr = propertyExpression.Body switch
        {
            MemberExpression m => m,
            UnaryExpression u when u.Operand is MemberExpression m => m,
            _ => null
        };

        if (memberExpr?.Member is not PropertyInfo prop)
        {
            throw new ArgumentException($"Expression '{propertyExpression}' does not refer to a property on '{typeof(TRecord).Name}'.");
        }

        return prop;
    }
}
