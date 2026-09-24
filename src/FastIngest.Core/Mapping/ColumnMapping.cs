using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

namespace FastIngest.Core.Mapping;

/// <summary>
/// Represents the mapping between a strongly-typed record property and a source/target tabular column.
/// </summary>
/// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
public sealed class ColumnMapping<TRecord>
{
    /// <summary>
    /// Gets the reflection metadata of the mapped property.
    /// </summary>
    public PropertyInfo Property { get; }

    /// <summary>
    /// Gets the CLR name of the mapped property.
    /// </summary>
    public string PropertyName => Property.Name;

    /// <summary>
    /// Gets the column header name in the source file or destination database table.
    /// </summary>
    public string ColumnName { get; }

    /// <summary>
    /// Gets the zero-based column ordinal in the tabular file, if explicitly configured.
    /// </summary>
    public int? ColumnIndex { get; }

    /// <summary>
    /// Gets the <see cref="Type"/> of the mapped property.
    /// </summary>
    public Type PropertyType => Property.PropertyType;

    /// <summary>
    /// Gets the pre-compiled delegate for high-throughput property value extraction.
    /// </summary>
    public Func<TRecord, object?> Getter { get; }

    /// <summary>
    /// Gets the pre-compiled delegate for mutating the property on an existing instance, if writable.
    /// </summary>
    public Action<TRecord, object?>? Setter { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ColumnMapping{TRecord}"/> class.
    /// </summary>
    /// <param name="property">The target property reflection info.</param>
    /// <param name="columnName">The matching tabular column name.</param>
    /// <param name="columnIndex">The optional explicit zero-based column index.</param>
    public ColumnMapping(PropertyInfo property, string columnName, int? columnIndex = null)
    {
        Property = property ?? throw new ArgumentNullException(nameof(property));
        ColumnName = string.IsNullOrWhiteSpace(columnName) ? property.Name : columnName;
        ColumnIndex = columnIndex;
        Getter = CreateGetter(property);
        Setter = CreateSetter(property);
    }

    /// <summary>
    /// Compiles a lambda expression into a fast property getter delegate to avoid reflection overhead during row processing.
    /// </summary>
    private static Func<TRecord, object?> CreateGetter(PropertyInfo property)
    {
        var param = Expression.Parameter(typeof(TRecord), "record");
        var propAccess = Expression.Property(param, property);
        var convert = Expression.Convert(propAccess, typeof(object));
        return Expression.Lambda<Func<TRecord, object?>>(convert, param).Compile();
    }

    /// <summary>
    /// Compiles a lambda expression into a fast property setter delegate to avoid reflection overhead during row materialization.
    /// </summary>
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

/// <summary>
/// Fluent builder used to define property-to-column mappings for a record type.
/// </summary>
/// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
public sealed class ColumnMappingBuilder<TRecord>
{
    private readonly Dictionary<string, ColumnMapping<TRecord>> _mappings = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Maps a record property to a source column by header name using a strongly-typed expression.
    /// </summary>
    /// <typeparam name="TProperty">The property value type.</typeparam>
    /// <param name="propertyExpression">An expression selecting the target property, e.g. <c>x => x.Email</c>.</param>
    /// <param name="columnName">The name of the column in the CSV/source data. If omitted, the property name is used.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public ColumnMappingBuilder<TRecord> Map<TProperty>(
        Expression<Func<TRecord, TProperty>> propertyExpression,
        string? columnName = null)
    {
        var property = ExtractProperty(propertyExpression);
        var mapping = new ColumnMapping<TRecord>(property, columnName ?? property.Name);
        _mappings[property.Name] = mapping;
        return this;
    }

    /// <summary>
    /// Maps a record property to a source column by zero-based ordinal index using a strongly-typed expression.
    /// </summary>
    /// <typeparam name="TProperty">The property value type.</typeparam>
    /// <param name="propertyExpression">An expression selecting the target property, e.g. <c>x => x.Id</c>.</param>
    /// <param name="columnIndex">The zero-based index of the column in the source data.</param>
    /// <param name="columnName">Optional column name override for logging and sink destination targeting.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
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

    /// <summary>
    /// Finalizes and returns the collection of defined column mappings.
    /// If no explicit mappings were configured, default mappings for all public properties are produced.
    /// </summary>
    /// <returns>A read-only collection of <see cref="ColumnMapping{TRecord}"/> items.</returns>
    public IReadOnlyList<ColumnMapping<TRecord>> Build()
    {
        if (_mappings.Count > 0)
        {
            return _mappings.Values.ToList();
        }

        return CreateDefaultMappings();
    }

    /// <summary>
    /// Automatically reflects all public instance properties on <typeparamref name="TRecord"/>
    /// and generates 1:1 default column mappings.
    /// </summary>
    /// <returns>A read-only collection of default column mappings.</returns>
    public static IReadOnlyList<ColumnMapping<TRecord>> CreateDefaultMappings()
    {
        var properties = typeof(TRecord).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        return properties.Select(p => new ColumnMapping<TRecord>(p, p.Name)).ToList();
    }

    /// <summary>
    /// Extracts the target <see cref="PropertyInfo"/> from a member or unary expression.
    /// </summary>
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
