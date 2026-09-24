using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace FastIngest.Extensions.DependencyInjection.Profiles;

/// <summary>
/// Defines the contract for looking up and registering FastIngest profiles keyed by record type.
/// </summary>
public interface IFastIngestProfileRegistry
{
    /// <summary>
    /// Registers a profile and pre-compiles its mapping expressions.
    /// </summary>
    /// <param name="profile">The profile instance to register.</param>
    void Register(IFastIngestProfile profile);

    /// <summary>
    /// Registers a strongly-typed profile and pre-compiles its mapping expressions.
    /// </summary>
    /// <typeparam name="TRecord">The domain record type.</typeparam>
    /// <param name="profile">The profile instance to register.</param>
    void Register<TRecord>(FastIngestProfile<TRecord> profile);

    /// <summary>
    /// Retrieves the registered profile for <typeparamref name="TRecord"/>, or null if not registered.
    /// </summary>
    /// <typeparam name="TRecord">The domain record type.</typeparam>
    /// <returns>The registered profile, or null.</returns>
    FastIngestProfile<TRecord>? GetProfile<TRecord>();

    /// <summary>
    /// Retrieves the registered profile for the given record type, or null if not registered.
    /// </summary>
    /// <param name="recordType">The domain record type.</param>
    /// <returns>The registered profile, or null.</returns>
    IFastIngestProfile? GetProfile(Type recordType);

    /// <summary>
    /// Attempts to retrieve the registered profile for <typeparamref name="TRecord"/>.
    /// </summary>
    /// <typeparam name="TRecord">The domain record type.</typeparam>
    /// <param name="profile">When found, contains the registered profile.</param>
    /// <returns>True if a profile is registered; otherwise, false.</returns>
    bool TryGetProfile<TRecord>([NotNullWhen(true)] out FastIngestProfile<TRecord>? profile);
}

/// <summary>
/// Singleton registry and cache holding configured profiles keyed by <see cref="Type"/>.
/// Caches pre-compiled property expression delegates for high-throughput ingestion.
/// </summary>
public class FastIngestProfileRegistry : IFastIngestProfileRegistry
{
    private readonly ConcurrentDictionary<Type, IFastIngestProfile> _profiles = new();

    /// <inheritdoc/>
    public void Register(IFastIngestProfile profile)
    {
        ArgumentNullException.ThrowIfNull(profile);

        // Pre-compile expressions eagerly if generic profile
        var getMappingsMethod = profile.GetType().GetMethod(nameof(FastIngestProfile<object>.GetMappings));
        getMappingsMethod?.Invoke(profile, null);

        _profiles[profile.RecordType] = profile;
    }

    /// <inheritdoc/>
    public void Register<TRecord>(FastIngestProfile<TRecord> profile)
    {
        ArgumentNullException.ThrowIfNull(profile);

        // Warm up and pre-compile expressions immediately
        _ = profile.GetMappings();

        _profiles[typeof(TRecord)] = profile;
    }

    /// <inheritdoc/>
    public FastIngestProfile<TRecord>? GetProfile<TRecord>()
    {
        if (_profiles.TryGetValue(typeof(TRecord), out var profile) && profile is FastIngestProfile<TRecord> typedProfile)
        {
            return typedProfile;
        }

        return null;
    }

    /// <inheritdoc/>
    public IFastIngestProfile? GetProfile(Type recordType)
    {
        ArgumentNullException.ThrowIfNull(recordType);
        _profiles.TryGetValue(recordType, out var profile);
        return profile;
    }

    /// <inheritdoc/>
    public bool TryGetProfile<TRecord>([NotNullWhen(true)] out FastIngestProfile<TRecord>? profile)
    {
        profile = GetProfile<TRecord>();
        return profile != null;
    }

    /// <summary>
    /// Gets all registered profiles.
    /// </summary>
    public IReadOnlyCollection<IFastIngestProfile> GetAllProfiles() => _profiles.Values.ToList().AsReadOnly();
}
