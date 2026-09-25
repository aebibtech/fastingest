using System.Text;
using FastIngest.Core.Common;
using FastIngest.Core.Exceptions;
using FastIngest.Core.Pipeline;
using FastIngest.Core.Sinks;
using FastIngest.Extensions.DependencyInjection;
using FastIngest.Extensions.DependencyInjection.Options;
using FastIngest.Extensions.DependencyInjection.Profiles;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace FastIngest.Tests;

public record DiCustomer(int Id, string Email, string FullName, decimal Balance);

public class DiCustomerProfile : FastIngestProfile<DiCustomer>
{
    public DiCustomerProfile()
    {
        ToTable("di_customers");
        WithBatchSize(2);
        WithErrorStrategy(ErrorStrategy.CollectAndContinue);
        WithFileType(FileType.Csv);

        Map(x => x.Id, "customer_id");
        Map(x => x.Email, "email");
        Map(x => x.FullName, "full_name");
        Map(x => x.Balance, "balance");
    }
}

public class DiCustomerFailFastProfile : FastIngestProfile<DiCustomer>
{
    public DiCustomerFailFastProfile()
    {
        ToTable("di_customers");
        WithBatchSize(2);
        WithErrorStrategy(ErrorStrategy.FailFast);
        WithFileType(FileType.Csv);

        Map(x => x.Id, "customer_id");
        Map(x => x.Email, "email");
        Map(x => x.FullName, "full_name");
        Map(x => x.Balance, "balance");
    }
}

public class DiCustomerCustomCapacityProfile : FastIngestProfile<DiCustomer>
{
    public DiCustomerCustomCapacityProfile()
    {
        ToTable("di_customers");
        WithBatchSize(2);
        WithChannelCapacity(4);
        WithErrorStrategy(ErrorStrategy.CollectAndContinue);
        WithFileType(FileType.Csv);

        Map(x => x.Id, "customer_id");
        Map(x => x.Email, "email");
        Map(x => x.FullName, "full_name");
        Map(x => x.Balance, "balance");
    }
}

public class DiCustomerJsonLinesProfile : FastIngestProfile<DiCustomer>
{
    public DiCustomerJsonLinesProfile()
    {
        ToTable("di_customers");
        WithBatchSize(2);
        WithErrorStrategy(ErrorStrategy.CollectAndContinue);
        WithFileType(FileType.JsonLines);
        WithJsonOptions(new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true });
    }
}

public class DiCustomerValidator : AbstractValidator<DiCustomer>
{
    public DiCustomerValidator()
    {
        RuleFor(x => x.Email).NotEmpty().EmailAddress();
        RuleFor(x => x.FullName).NotEmpty();
        RuleFor(x => x.Balance).GreaterThanOrEqualTo(0);
    }
}

public record UnregisteredRecord(string Name);

public class DependencyInjectionTests
{
    [Fact]
    public void ProfileRegistry_Should_Register_And_Retrieve_Profile()
    {
        var registry = new FastIngestProfileRegistry();
        var profile = new DiCustomerProfile();

        registry.Register(profile);

        var retrieved = registry.GetProfile<DiCustomer>();
        Assert.NotNull(retrieved);
        Assert.Equal("di_customers", retrieved.TargetTable);
        Assert.Equal(2, retrieved.BatchSize);
        Assert.Equal(ErrorStrategy.CollectAndContinue, retrieved.ErrorStrategy);

        // Pre-compiled mappings check
        var mappings = retrieved.GetMappings();
        Assert.Equal(4, mappings.Count);
        Assert.NotNull(mappings[0].Getter);
    }

    [Fact]
    public void ProfileRegistry_Should_Return_Null_For_Unregistered_Type()
    {
        var registry = new FastIngestProfileRegistry();
        var profile = registry.GetProfile<UnregisteredRecord>();
        Assert.Null(profile);
    }

    [Fact]
    public void ServiceCollection_AddFastIngest_Should_Register_Core_Services()
    {
        var services = new ServiceCollection();
        services.AddFastIngest(builder =>
        {
            builder.AddPostgreSqlSink("Host=localhost;Database=test;Username=postgres;Password=postgres");
            builder.RegisterProfile<DiCustomerProfile>();
        });

        using var serviceProvider = services.BuildServiceProvider();

        var registry = serviceProvider.GetService<FastIngestProfileRegistry>();
        Assert.NotNull(registry);

        var profile = registry.GetProfile<DiCustomer>();
        Assert.NotNull(profile);

        using var scope = serviceProvider.CreateScope();
        var engine = scope.ServiceProvider.GetService<IFastIngestEngine>();
        Assert.NotNull(engine);
    }

    [Fact]
    public void ServiceCollection_RegisterProfilesFromAssembly_Should_Discover_Profiles()
    {
        var services = new ServiceCollection();
        services.AddFastIngest().RegisterProfilesFromAssembly(typeof(DependencyInjectionTests).Assembly);

        using var serviceProvider = services.BuildServiceProvider();
        var registry = serviceProvider.GetRequiredService<FastIngestProfileRegistry>();

        var profile = registry.GetProfile<DiCustomer>();
        Assert.NotNull(profile);
    }

    [Fact]
    public async Task Engine_Should_Ingest_Successfully_Using_Profile_And_Resolved_Validator()
    {
        var csv = """
                  customer_id,email,full_name,balance
                  1,alice@example.com,Alice Smith,150.50
                  2,bob@example.com,Bob Jones,200.00
                  3,invalid-email,Carol White,-10.00
                  """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));

        var services = new ServiceCollection();
        services.AddFastIngest(b => b.RegisterProfile<DiCustomerProfile>());
        services.AddScoped<IValidator<DiCustomer>, DiCustomerValidator>();

        using var serviceProvider = services.BuildServiceProvider();
        using var scope = serviceProvider.CreateScope();

        var engine = scope.ServiceProvider.GetRequiredService<IFastIngestEngine>();
        var sink = new TestMemorySink<DiCustomer>();
        var progressList = new List<IngestProgress>();

        var result = await engine.IngestAsync(stream, sink, onProgress: p => progressList.Add(p));

        Assert.Equal(3, result.TotalProcessed);
        Assert.Equal(2, result.TotalSucceeded);
        Assert.Equal(1, result.TotalFailed);
        Assert.False(result.IsSuccess);
        Assert.Equal(2, result.Errors.Count);
        Assert.Equal(2, sink.Records.Count);
        Assert.NotEmpty(progressList);
    }

    [Fact]
    public async Task Engine_Should_Throw_On_FailFast_Profile_When_Validation_Fails()
    {
        var csv = """
                  customer_id,email,full_name,balance
                  1,invalid-email,Alice Smith,150.50
                  """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));

        var services = new ServiceCollection();
        services.AddFastIngest(b => b.RegisterProfile<DiCustomerFailFastProfile>());
        services.AddScoped<IValidator<DiCustomer>, DiCustomerValidator>();

        using var serviceProvider = services.BuildServiceProvider();
        using var scope = serviceProvider.CreateScope();

        var engine = scope.ServiceProvider.GetRequiredService<IFastIngestEngine>();
        var sink = new TestMemorySink<DiCustomer>();

        await Assert.ThrowsAsync<FastIngestValidationException>(async () =>
        {
            await engine.IngestAsync(stream, sink);
        });
    }

    [Fact]
    public async Task Engine_Should_Throw_When_No_Profile_Registered()
    {
        var csv = "Name\nJohn";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));

        var services = new ServiceCollection();
        services.AddFastIngest();

        using var serviceProvider = services.BuildServiceProvider();
        using var scope = serviceProvider.CreateScope();

        var engine = scope.ServiceProvider.GetRequiredService<IFastIngestEngine>();
        var sink = new TestMemorySink<UnregisteredRecord>();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await engine.IngestAsync(stream, sink);
        });

        Assert.Contains("No FastIngestProfile found for record type", ex.Message);
    }

    [Fact]
    public async Task Engine_Should_Throw_When_No_ConnectionString_Or_Sink_Configured()
    {
        var csv = """
                  customer_id,email,full_name,balance
                  1,alice@example.com,Alice Smith,150.50
                  """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));

        var services = new ServiceCollection();
        // Register profile with no connection string and options with no connection string
        services.AddFastIngest(b => b.RegisterProfile<DiCustomerProfile>());

        using var serviceProvider = services.BuildServiceProvider();
        using var scope = serviceProvider.CreateScope();

        var engine = scope.ServiceProvider.GetRequiredService<IFastIngestEngine>();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            // Call IngestAsync without sink to trigger postgres resolution
            await engine.IngestAsync<DiCustomer>(stream);
        });

        Assert.Contains("No connection string configured", ex.Message);
    }

    [Fact]
    public void FastIngestOptions_Should_Expose_ChannelCapacity()
    {
        var options = new FastIngestOptions();
        Assert.Equal(2, options.ChannelCapacity);
        Assert.Equal(2, options.DefaultChannelCapacity);

        options.ChannelCapacity = 5;
        Assert.Equal(5, options.ChannelCapacity);
        Assert.Equal(5, options.DefaultChannelCapacity);

        options.DefaultChannelCapacity = 8;
        Assert.Equal(8, options.ChannelCapacity);
        Assert.Equal(8, options.DefaultChannelCapacity);
    }

    [Fact]
    public void FastIngestProfile_Should_Configure_ChannelCapacity()
    {
        var profile = new DiCustomerCustomCapacityProfile();
        Assert.Equal(4, profile.ChannelCapacity);
    }

    [Fact]
    public async Task Engine_Should_Ingest_Successfully_With_Custom_ChannelCapacity()
    {
        var csv = """
                  customer_id,email,full_name,balance
                  1,alice@example.com,Alice Smith,150.50
                  2,bob@example.com,Bob Jones,200.00
                  """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));

        var services = new ServiceCollection();
        services.AddFastIngest(b => b.RegisterProfile<DiCustomerCustomCapacityProfile>());

        using var serviceProvider = services.BuildServiceProvider();
        using var scope = serviceProvider.CreateScope();

        var engine = scope.ServiceProvider.GetRequiredService<IFastIngestEngine>();
        var sink = new TestMemorySink<DiCustomer>();

        var result = await engine.IngestAsync(stream, sink);

        Assert.Equal(2, result.TotalProcessed);
        Assert.Equal(2, result.TotalSucceeded);
        Assert.True(result.IsSuccess);
    }

    [Fact]
    public async Task Engine_Should_Ingest_JsonLines_Successfully_Using_Profile()
    {
        var ndjson = """
                     {"id": 1, "email": "alice@example.com", "fullName": "Alice Smith", "balance": 150.50}
                     {"id": 2, "email": "bob@example.com", "fullName": "Bob Jones", "balance": 200.00}
                     """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(ndjson));

        var services = new ServiceCollection();
        services.AddFastIngest(b => b.RegisterProfile<DiCustomerJsonLinesProfile>());

        using var serviceProvider = services.BuildServiceProvider();
        using var scope = serviceProvider.CreateScope();

        var engine = scope.ServiceProvider.GetRequiredService<IFastIngestEngine>();
        var sink = new TestMemorySink<DiCustomer>();

        var result = await engine.IngestAsync(stream, sink);

        Assert.Equal(2, result.TotalProcessed);
        Assert.Equal(2, result.TotalSucceeded);
        Assert.True(result.IsSuccess);
        Assert.Equal(2, sink.Records.Count);
        Assert.Equal("alice@example.com", sink.Records[0].Email);
        Assert.Equal("bob@example.com", sink.Records[1].Email);
    }
}
