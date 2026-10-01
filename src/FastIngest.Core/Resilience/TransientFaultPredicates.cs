namespace FastIngest.Core.Resilience;

/// <summary>
/// Built-in transient fault predicates for common database exceptions.
/// Evaluated dynamically by property reflection/pattern matching to avoid hard dependencies on specific database driver assemblies.
/// </summary>
public static class TransientFaultPredicates
{
    /// <summary>
    /// Determines whether the specified exception represents a known transient database or I/O failure.
    /// </summary>
    /// <param name="ex">The exception to evaluate.</param>
    /// <returns><c>true</c> if the exception is considered transient; otherwise, <c>false</c>.</returns>
    public static bool IsTransient(Exception ex)
    {
        if (ex is null) return false;

        if (ex is TimeoutException) return true;

        var type = ex.GetType();
        var fullName = type.FullName;

        // Npgsql: check IsTransient property (e.g. Npgsql.NpgsqlException)
        if (fullName is "Npgsql.NpgsqlException")
        {
            var isTransientProp = type.GetProperty("IsTransient");
            if (isTransientProp?.GetValue(ex) is true)
            {
                return true;
            }
        }

        // SQL Server: Microsoft.Data.SqlClient.SqlException or System.Data.SqlClient.SqlException
        if (fullName is "Microsoft.Data.SqlClient.SqlException" or "System.Data.SqlClient.SqlException")
        {
            var numberProp = type.GetProperty("Number");
            if (numberProp?.GetValue(ex) is int number)
            {
                // 1205: Deadlock victim
                // -2: Execution timeout
                // 233: Connection broken / pipe error
                // 10054: Connection reset by peer
                // 10060: Connection timed out
                // 4060: Cannot open database
                // 40197, 40501, 40613, 49918, 49919, 49920, 4221: Azure SQL transient errors
                if (number is 1205 or -2 or 233 or 10054 or 10060 or 4060 or 40197 or 40501 or 40613 or 49918 or 49919 or 49920 or 4221)
                {
                    return true;
                }
            }

            var isTransientProp = type.GetProperty("IsTransient");
            if (isTransientProp?.GetValue(ex) is true)
            {
                return true;
            }
        }

        // MySqlConnector / MySQL: check SqlState (08xxx is connection exception) or IsTransient
        if (fullName?.StartsWith("MySqlConnector.", StringComparison.Ordinal) == true ||
            fullName?.StartsWith("MySql.Data.", StringComparison.Ordinal) == true)
        {
            var isTransientProp = type.GetProperty("IsTransient");
            if (isTransientProp?.GetValue(ex) is true)
            {
                return true;
            }

            var sqlStateProp = type.GetProperty("SqlState");
            if (sqlStateProp?.GetValue(ex) is string sqlState &&
                sqlState.StartsWith("08", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
